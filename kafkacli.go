package main

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"strings"

	"github.com/bsm/sarama-cluster"
	"github.com/pkg/errors"
	"github.com/satori/go.uuid"

	"github.com/Shopify/sarama"

	"github.com/jawher/mow.cli"
)

func main() {
	app := cli.App("kafkacli", "Kafkacli")
	app.Spec = "[-b] -t... [--from-beginning] [-g] [-m] [-h...] [-e] [-s] [--ssl-cafile] [--ssl-certfile] [--ssl-keyfile]"
	var (
		bootstrapServers   = app.StringOpt("b broker brokers", "localhost:9092", "brokers")
		topics             = app.StringsOpt("t topic", nil, "topic")
		fromBeginning      = app.BoolOpt("from-beginning", false, "start with the earliest message")
		consumerGroupId    = app.StringOpt("g consumer-group", "", "consumer group id")
		message            = app.StringOpt("m message", "", "message message")
		headers            = app.StringsOpt("h header", nil, "message header <key=value>")
		existOnLastMessage = app.BoolOpt("e exit", false, "exit when last message received")
		useSSL             = app.BoolOpt("s secure", false, "use SSL")
		sslCAFile          = app.StringOpt("ssl-cafile", "", "filename of CA file to use in certificate verification")
		sslCertFile        = app.StringOpt("ssl-certfile", "", "filename of file in PEM format containing the client certificate")
		sslKeyFile         = app.StringOpt("ssl-keyfile", "", "filename containing the client private key")
	)

	app.Action = func() {
		config := config(*useSSL, *sslCAFile, *sslCertFile, *sslKeyFile)

		if *message != "" {
			produce(*config, *bootstrapServers, *topics, *headers, *message)
		} else {
			consume(*config, *bootstrapServers, *topics, *fromBeginning, *consumerGroupId, *existOnLastMessage)
		}
	}

	die(app.Run(os.Args))
}

func config(useSSL bool, sslCAFile string, sslCertFile string, sslKeyFile string) *cluster.Config {
	config := cluster.NewConfig()
	config.Version = sarama.V1_0_0_0
	config.Group.Return.Notifications = true

	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetNewest

	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 10
	config.Producer.Return.Successes = true

	if useSSL || sslCAFile != "" || sslCertFile != "" || sslKeyFile != "" {
		config.Net.TLS.Enable = true
	}

	config.Net.TLS.Config = &tls.Config{}

	if sslCertFile != "" || sslKeyFile != "" {
		if sslCertFile == "" && sslKeyFile == "" {
			die(errors.New("You need to specify both ssl-certfile and ssl-keyfile"))
		}

		cer, err := tls.LoadX509KeyPair(sslCertFile, sslKeyFile)
		die(err)

		config.Net.TLS.Config.Certificates = []tls.Certificate{cer}
	}

	if sslCAFile != "" {
		caCert, err := ioutil.ReadFile(sslCAFile)
		die(err)

		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)
		config.Net.TLS.Config.RootCAs = caCertPool
	}
	return config
}

func consume(config cluster.Config, bootstrapServers string, topics []string, fromBeginning bool, consumerGroupId string, existOnLastMessage bool) {
	fmt.Printf("Topics: %v from %v\n", topics, bootstrapServers)

	if fromBeginning {
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	}

	if consumerGroupId == "" {
		uuidString := uuid.NewV4().String()
		consumerGroupId = uuidString
	}
	consumer, err := cluster.NewConsumer(strings.Split(bootstrapServers, ","), consumerGroupId, topics, &config)
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

	startConsuming := make(chan struct{})
	var partitionToRead int

	// consume notifications
	go func() {
		for ntf := range consumer.Notifications() {
			log.Printf("Rebalanced: %+v\n", ntf)
			if len(ntf.Claimed) != 0 {
				for _, topic := range ntf.Claimed {
					partitionToRead += len(topic)
				}
				startConsuming <- struct{}{}
			}
		}
	}()

	// consume messages, watch signals
	<-startConsuming

	var messageCount int

	for {
		select {
		case msg, ok := <-consumer.Messages():
			if ok {
				displayMessage(msg)
				messageCount++
				marks := consumer.HighWaterMarks()
				if existOnLastMessage && msg.Offset+1 == marks[msg.Topic][msg.Partition] {
					partitionToRead -= 1
				}
			}
		case <-signals:
			partitionToRead = 0
		}

		if partitionToRead == 0 {
			break
		}
	}
	log.Printf("%d messages received\n", messageCount)
}

func produce(config cluster.Config, bootstrapServers string, topics []string, headers []string, message string) {
	producer, err := sarama.NewSyncProducer(strings.Split(bootstrapServers, ","), &config.Config)
	die(err)

	defer func() {
		if err := producer.Close(); err != nil {
			log.Printf("error while closing producer: %+v\n", err)
		}
	}()

	var kafkaHeaders []sarama.RecordHeader
	for _, element := range headers {
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

	for _, topic := range topics {
		message := sarama.ProducerMessage{
			Topic:   topic,
			Headers: kafkaHeaders,
			Value:   sarama.StringEncoder(message),
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
