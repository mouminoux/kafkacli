package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/Shopify/sarama"

	cluster "github.com/bsm/sarama-cluster"

	"github.com/jawher/mow.cli"
	"github.com/satori/go.uuid"
)

func main() {
	app := cli.App("kafkacli", "Kafka consumer")
	app.Spec = "[-b] -t..."
	var (
		bootstrapServers = app.StringOpt("b broker brokers", "localhost:9092", "brokers")
		topics           = app.StringsOpt("t topic", nil, "topic")
	)

	app.Action = func() {
		fmt.Printf("Topics: %v from %v", *topics, *bootstrapServers)

		groupID, err := uuid.NewV4()
		die(err)

		config := cluster.NewConfig()
		config.Version = sarama.V1_0_0_0
		config.Consumer.Return.Errors = true
		config.Group.Return.Notifications = true
		config.Consumer.Offsets.Initial = sarama.OffsetNewest

		consumer, err := cluster.NewConsumer([]string{*bootstrapServers}, groupID.String(), *topics, config)
		die(err)

		defer consumer.Close()

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

					fmt.Printf("[%s]----------------\n", msg.Timestamp)
					fmt.Printf("Headers on %s/%d:", msg.Topic, msg.Partition)
					for _, header := range msg.Headers {
						fmt.Printf(" %s=%s", header.Key, header.Value)
					}
					fmt.Printf("\n")

					fmt.Printf("Message on %s/%d: [%s]%s\n", msg.Topic, msg.Partition, msg.Key, msg.Value)

					// fmt.Fprintf(os.Stdout, "%s/%d/%d\t%s\t%s\t%v\n", msg.Topic, msg.Partition, msg.Offset, msg.Key, msg.Value, msg.Headers)
					consumer.MarkOffset(msg, "") // mark message as processed
				}
			case <-signals:
				return
			}
		}
	}

	app.Run(os.Args)
}

func die(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		cli.Exit(1)
	}
}
