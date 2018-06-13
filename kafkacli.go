package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/satori/go.uuid"
	"os"
	"strings"
)

func main() {
	args := os.Args
	bootstrapServers := args[1]
	topics := strings.Split(args[2], ",")

	groupId, _ := uuid.NewV4()

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
		"group.id":          groupId,
		"auto.offset.reset": "earliest",
	})

	if err != nil {
		panic(err)
	}

	c.SubscribeTopics(topics, nil)

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
