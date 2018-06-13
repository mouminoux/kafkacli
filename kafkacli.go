package main

import (
	"fmt"
	"os"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/satori/go.uuid"
	"github.com/jawher/mow.cli"
)

func main() {
	app := cli.App("kafkacli", "Kafka consumer")
	app.Spec = "[-b] -t..."
	var (
		bootstrapServers = app.StringOpt("b broker brokers", "localhost:9092", "brokers")
		topics           = app.StringsOpt("t topic", nil, "topic")
	)

	app.Action = func() {
		fmt.Printf("Topics: %s from %s", topics, *bootstrapServers)

		groupId, _ := uuid.NewV4()

		c, err := kafka.NewConsumer(&kafka.ConfigMap{
			"bootstrap.servers": *bootstrapServers,
			"group.id":          groupId,
			"auto.offset.reset": "earliest",
		})

		if err != nil {
			panic(err)
		}

		c.SubscribeTopics(*topics, nil)

		for {
			msg, err := c.ReadMessage(-1)

			if err != nil {
				fmt.Printf("Consumer error: %v (%v)\n", err, msg)
				break
			}

			fmt.Printf("[%s]----------------\n", msg.Timestamp)
			fmt.Printf("Headers on %s:", msg.TopicPartition)
			for _, header := range msg.Headers {
				fmt.Printf(" %s=%s", header.Key, string(header.Value))
			}
			fmt.Printf("\n")

			fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
		}

		c.Close()
	}

	app.Run(os.Args)
}
