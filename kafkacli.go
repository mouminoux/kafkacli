package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"

	cluster "github.com/bsm/sarama-cluster"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"

	"github.com/Shopify/sarama"

	cli "github.com/jawher/mow.cli"
)

func main() {
	app := cli.App("kafkacli", "Kafkacli")
	app.Spec = "[-b] -t... [--from-beginning] [-g] [-m] [-h...]"
	var (
		bootstrapServers = app.StringOpt("b broker brokers", "localhost:9092", "brokers")
		topics           = app.StringsOpt("t topic", nil, "topic")
		fromBeginning    = app.BoolOpt("from-beginning", false, "start with the earliest message")
		consumerGroupId  = app.StringOpt("g consumer-group", "", "consumer group id")
		message          = app.StringOpt("m message", "", "message message")
		headers          = app.StringsOpt("h header", nil, "message header <key=value>")
	)

	app.Action = func() {
		if *message != "" {
			produce(bootstrapServers, topics, headers, message)
		} else {
			consume(bootstrapServers, topics, fromBeginning, consumerGroupId)
		}

	}

	die(app.Run(os.Args))
}

func consume(bootstrapServers *string, topics *[]string, fromBeginning *bool, consumerGroupId *string) {
	fmt.Printf("Topics: %v from %v", *topics, *bootstrapServers)

	config := cluster.NewConfig()
	config.Version = sarama.V1_0_0_0
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true
	config.Consumer.Offsets.Initial = sarama.OffsetNewest

	if *fromBeginning {
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	}

	if *consumerGroupId == "" {
		uuidString := uuid.NewV4().String()
		consumerGroupId = &uuidString
	}
	consumer, err := cluster.NewConsumer(strings.Split(*bootstrapServers, ","), *consumerGroupId, *topics, config)
	die(err)

	defer func() {
		if err := consumer.Close(); err != nil {
			log.Printf("error while closing consumer: %+v\n", err)
		}
	}()

	// trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	// consume errors
	go func() {
		for err := range consumer.Errors() {
			log.Printf("Error: %v\n", err)
		}
	}()

	// consume notifications
	go func() {
		for ntf := range consumer.Notifications() {
			log.Printf("Rebalanced: %+v\n", ntf)
		}
	}()

	// consume messages, watch signals
	for {
		select {
		case msg, ok := <-consumer.Messages():
			if ok {
				displayMessage(msg)
			}
		case <-signals:
			return
		}
	}
}

func produce(bootstrapServers *string, topics *[]string, headers *[]string, message *string) {
	config := sarama.NewConfig()
	config.Version = sarama.V1_0_0_0
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 10
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer(strings.Split(*bootstrapServers, ","), config)
	die(err)

	defer func() {
		if err := producer.Close(); err != nil {
			log.Printf("error while closing producer: %+v\n", err)
		}
	}()

	var kafkaHeaders []sarama.RecordHeader
	for _, element := range *headers {
		headerKeyValue := strings.Split(element, "=")
		if len(headerKeyValue) != 2 {
			die(errors.New("Invalid header param"))
		}

		headerKey := headerKeyValue[0]
		headerValue := headerKeyValue[1]

		newHeader := sarama.RecordHeader{
			Key:   []byte(headerKey),
			Value: []byte(headerValue),
		}
		kafkaHeaders = append(kafkaHeaders, newHeader)
	}

	for _, topic := range *topics {
		message := sarama.ProducerMessage{
			Topic:   topic,
			Headers: kafkaHeaders,
			Value:   sarama.StringEncoder(*message),
		}

		log.Printf("Send msg %+v\n", message)
		_, _, err = producer.SendMessage(&message)
		if err != nil {
			log.Printf("%+v\n", err)
		}
	}
}

func displayMessage(msg *sarama.ConsumerMessage) {
	fmt.Printf("[%s] %s/%d----------------\n", msg.Timestamp, msg.Topic, msg.Partition)
	fmt.Printf("Headers:")
	for _, header := range msg.Headers {
		fmt.Printf(" %s=%s", header.Key, header.Value)
	}
	fmt.Printf("\n")
	fmt.Printf("Message")
	if msg.Key != nil {
		fmt.Printf("[%s]%", msg.Key)
	}
	fmt.Printf(": %s\n", msg.Value)
}

func die(err error) {
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "error: %v\n", err)
		cli.Exit(1)
	}
}
