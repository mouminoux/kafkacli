package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/IBM/sarama"
	cli "github.com/jawher/mow.cli"
	"github.com/mouminoux/kafkacli/filter"
	uuid "github.com/satori/go.uuid"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
)

type stack []uintptr
type consumeError struct {
	message string
	cause   error
}

func (f *consumeError) Error() string { return f.message }

var messageCount int

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
		consume(cfg, splitFlatten(*bootstrapServers), splitFlatten(*topics), *prettyPrint, *fromBeginning, *consumerGroupId, *existOnLastMessage, f)
	}
}

func consume(config *sarama.Config,
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

	/**
	 * Setup a new Sarama consumer group
	 */
	consumer := Consumer{
		ready:       make(chan bool),
		filter:      f,
		prettyPrint: prettyPrint,
	}

	ctx, cancel := context.WithCancel(context.Background())
	consumerGroup, err := sarama.NewConsumerGroup(bootstrapServers, consumerGroupId, config)
	die(err)

	consumptionIsPaused := false
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			// `Consume` should be called inside an infinite loop, when a
			// server-side rebalance happens, the consumer session will need to be
			// recreated to get the new claims
			if err := consumerGroup.Consume(ctx, topics, &consumer); err != nil {
				if errors.Is(err, sarama.ErrClosedConsumerGroup) {
					return
				}
				log.Panicf("Error from consumer: %v", err)
			}
			// check if context was cancelled, signaling that the consumer should stop
			if ctx.Err() != nil {
				return
			}
			consumer.ready = make(chan bool)
		}
	}()
	<-consumer.ready // Await till the consumer has been set up
	log.Println("Sarama consumer up and running!...")

	sigusr1 := make(chan os.Signal, 1)
	signal.Notify(sigusr1, syscall.SIGUSR1)

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

	keepRunning := true
	for keepRunning {
		select {
		case <-ctx.Done():
			log.Println("terminating: context cancelled")
			keepRunning = false
		case <-sigterm:
			log.Println("terminating: via signal")
			keepRunning = false
		case <-sigusr1:
			toggleConsumptionFlow(consumerGroup, &consumptionIsPaused)
		}
	}

	log.Printf("%d messages received\n", messageCount)

	cancel()
	wg.Wait()
	if err = consumerGroup.Close(); err != nil {
		log.Panicf("Error closing client: %v", err)
	}
}

func toggleConsumptionFlow(client sarama.ConsumerGroup, isPaused *bool) {
	if *isPaused {
		client.ResumeAll()
		log.Println("Resuming consumption")
	} else {
		client.PauseAll()
		log.Println("Pausing consumption")
	}

	*isPaused = !*isPaused
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
			return nil, &consumeError{
				message: fmt.Sprintf("Invalid filter %q. must be in <filter-type>:<filter-condition> format", fs),
			}
		}
		switch parts[0] {
		case "header", "h":
			k, v, err := parseKEqV(parts[1])
			if err != nil {
				return nil, &consumeError{
					message: fmt.Sprintf("Invalid filter %q", fs),
					cause:   err,
				}
			}
			ff[i] = filter.Header(k, v)
		default:
			return nil, &consumeError{
				message: fmt.Sprintf("Unknown filter type %q in filter %q", parts[0], fs),
			}
		}

	}
	return filter.Anded(ff), nil
}

func parseKEqV(s string) (string, string, error) {
	parts := strings.SplitN(s, "=", 2)
	if len(parts) != 2 {
		return "", "", &consumeError{
			message: fmt.Sprintf("Invalid filter condition %q. must be in <x>=<y> format", s),
		}
	}
	return parts[0], parts[1], nil
}

// Consumer represents a Sarama consumer group consumer
type Consumer struct {
	ready       chan bool
	filter      filter.Filter
	prettyPrint bool
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer *Consumer) Setup(session sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	close(consumer.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
// Once the Messages() channel is closed, the Handler must finish its processing
// loop and exit.
func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case msg, ok := <-claim.Messages():
			if !ok {
				continue
			}
			if !consumer.filter(msg) {
				continue
			}
			if consumer.prettyPrint {
				displayMessagePretty(msg)
			} else {
				displayMessageUgly(msg)
			}
			messageCount++
		case <-session.Context().Done():
			return nil
		}
	}
}
