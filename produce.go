package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	cli "github.com/jawher/mow.cli"
	"github.com/pkg/errors"
)

func produceCmd(c *cli.Cmd) {
	var (
		headers = c.StringsOpt("H header", nil, "message header <key=value>")
		message = c.StringOpt("m message", "", "message message")
		key     = c.StringOpt("k key", "", "key key")

		topics = c.Strings(cli.StringsArg{
			Name: "TOPIC",
			Desc: "topic(s) to consume from",
		})
	)

	c.Spec = "[-H...] [-m=<message|->]  [-k=<key>] TOPIC..."

	c.Action = func() {
		cfg := config(*useSSL, *sslCAFile, *sslCertFile, *sslKeyFile)

		if *message == "" || *message == "-" {
			*message = readStdin()
		}
		produce(*cfg, splitFlatten(*bootstrapServers), splitFlatten(*topics), *headers, *message, *key)
	}
}

func produce(config cluster.Config, bootstrapServers []string, topics []string, headers []string, message string, key string) {
	producer, err := sarama.NewSyncProducer(bootstrapServers, &config.Config)
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
		kafkaMessage := sarama.ProducerMessage{
			Topic:   topic,
			Headers: kafkaHeaders,
			Key:     sarama.StringEncoder(key),
			Value:   sarama.StringEncoder(message),
		}

		fmt.Printf("Send msg to topic %q:\n", topic)
		if len(kafkaHeaders) > 0 {
			fmt.Printf("(Headers):\n")
			for _, h := range kafkaHeaders {
				fmt.Printf("- %q: %q\n", h.Key, h.Value)
			}
		}
		fmt.Printf("(Payload):\n---\n%s\n---\n", message)
		var partition, offset, err = producer.SendMessage(&kafkaMessage)
		if err != nil {
			fmt.Printf("error: %+v\n", err)
		}
		fmt.Printf("Payload sent to partition %+v - resulting offset %+v\n", partition, offset)
	}
}

func readStdin() string {
	info, err := os.Stdin.Stat()
	if err != nil {
		die(err)
	}

	if info.Mode()&os.ModeNamedPipe == 0 {
		die(errors.Errorf(`a message body must be specified, either using -m="message content" or by piping the content: echo "message content | kafkacli send topic"`))
	}

	reader := bufio.NewReader(os.Stdin)
	var output strings.Builder

	for {
		input, _, err := reader.ReadRune()
		if err != nil && err == io.EOF {
			break
		}
		output.WriteRune(input)
	}

	return output.String()
}
