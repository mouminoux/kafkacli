package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	cli "github.com/jawher/mow.cli"
	"github.com/mouminoux/kafkacli/filter"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
)

func consumeCmd(c *cli.Cmd) {
	var (
		prettyPrint        = c.BoolOpt("p pretty-print", false, "pretty print the messages")
		fromBeginning      = c.BoolOpt("from-beginning", false, "start with the earliest message")
		consumerGroupId    = c.StringOpt("g consumer-group", "", "consumer group id. If unset, a random one will be generated")
		existOnLastMessage = c.BoolOpt("e exit", false, "exit when last message received")
		filters            = c.StringsOpt("filter", nil, `filter incoming messages against a set of conditions. Syntax: <filter-type>:<filter-condition>.
The currently supported filters:
* header filter: syntax: "header:<key>=<value>", e.g. --filter header:correlation-id=ac123-fds456`)

		topics = c.Strings(cli.StringsArg{
			Name: "TOPIC",
			Desc: "topic(s) to consume from",
		})
	)

	c.Spec = "[--from-beginning] [-g] [-e] [-p] [--filter...] TOPIC..."

	c.Action = func() {
		cfg := config(*useSSL, *sslCAFile, *sslCertFile, *sslKeyFile)
		f, err := parseFilters(*filters)
		die(err)
		consume(*cfg, splitFlatten(*bootstrapServers), splitFlatten(*topics), *prettyPrint, *fromBeginning, *consumerGroupId, *existOnLastMessage, f)
	}
}

func consume(config cluster.Config,
	bootstrapServers []string,
	topics []string,
	prettyPrint bool,
	fromBeginning bool,
	consumerGroupId string,
	existOnLastMessage bool,
	f filter.Filter) {
	fmt.Printf("Consuming from topic(s) %q, broker(s) %q\n", strings.Join(topics, ", "), strings.Join(bootstrapServers, ", "))

	if fromBeginning {
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	}

	if consumerGroupId == "" {
		uuidString := uuid.NewV4().String()
		consumerGroupId = uuidString
	}
	consumer, err := cluster.NewConsumer(bootstrapServers, consumerGroupId, topics, &config)
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
			fmt.Printf("Error: %v\n", err)
		}
	}()

	startConsuming := make(chan struct{})
	var partitionToRead int

	// consume notifications
	go func() {
		for ntf := range consumer.Notifications() {
			fmt.Printf("Rebalanced: %+v\n", ntf)
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
			if !ok {
				continue
			}
			if !f(msg) {
				continue
			}
			if prettyPrint {
				displayMessagePretty(msg)
			} else {
				displayMessageUgly(msg)
			}
			messageCount++
			marks := consumer.HighWaterMarks()
			if existOnLastMessage && msg.Offset+1 == marks[msg.Topic][msg.Partition] {
				partitionToRead -= 1
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

func displayMessagePretty(msg *sarama.ConsumerMessage) {
	fmt.Printf("---------------- [%v] %s/%d/%d ----------------\n", msg.Timestamp, msg.Topic, msg.Partition, msg.Offset)
	fmt.Printf("(Headers):\n")
	for _, header := range msg.Headers {
		fmt.Printf("- %q: %q\n", header.Key, header.Value)
	}
	if msg.Key != nil {
		fmt.Printf("\n(Key): %s\n", msg.Key)
	}
	fmt.Printf("\n(Payload):\n%s\n\n", msg.Value)
}

func displayMessageUgly(msg *sarama.ConsumerMessage) {
	fmt.Printf("[%s] %s/%d/%d ----------------\n", msg.Timestamp, msg.Topic, msg.Partition, msg.Offset)
	fmt.Printf("Headers:")
	for _, header := range msg.Headers {
		fmt.Printf(" %s=%s", header.Key, header.Value)
	}
	fmt.Printf("\n")
	fmt.Printf("Message")
	if msg.Key != nil {
		fmt.Printf("[%s]", msg.Key)
	}
	fmt.Printf(": %s\n", msg.Value)
}

func parseFilters(ffs []string) (filter.Filter, error) {
	ff := make([]filter.Filter, len(ffs))
	for i, fs := range ffs {
		parts := strings.SplitN(fs, ":", 2)
		if len(parts) != 2 {
			return nil, errors.Errorf("Invalid filter %q. must be in <filter-type>:<filter-condition> format", fs)
		}
		switch parts[0] {
		case "header", "h":
			k, v, err := parseKEqV(parts[1])
			if err != nil {
				return nil, errors.Wrapf(err, "Invalid filter %q", fs)
			}
			ff[i] = filter.Header(k, v)
		default:
			return nil, errors.Errorf("Unknown filter type %q in filter %q", parts[0], fs)
		}

	}
	return filter.Anded(ff), nil
}

func parseKEqV(s string) (string, string, error) {
	parts := strings.SplitN(s, "=", 2)
	if len(parts) != 2 {
		return "", "", errors.Errorf("Invalid filter condition %q. must be in <x>=<y> format", s)
	}
	return parts[0], parts[1], nil
}
