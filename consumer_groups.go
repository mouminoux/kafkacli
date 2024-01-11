package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	cli "github.com/jawher/mow.cli"
	"log"
	"sort"
	"strings"
)

func consumerGroupsCmd(c *cli.Cmd) {
	// add unique & sorted flags
	var (
		sorted = c.BoolOpt("s sorted", false, "sort the consumer groups (ascending)")
	)

	c.Action = func() {
		cfg := config(*useSSL, *sslCAFile, *sslCertFile, *sslKeyFile)
		consumerGroups(*cfg, splitFlatten(*bootstrapServers), sorted)
	}
}

func consumerGroups(config cluster.Config, bootstrapServers []string, sorted *bool) {
	fmt.Printf("Listing consumer groups for all topics on broker(s) %q\n", strings.Join(bootstrapServers, ", "))

	newAdmin, err := sarama.NewClusterAdmin(bootstrapServers, &config.Config)
	die(err)

	defer func() {
		if err := newAdmin.Close(); err != nil {
			log.Printf("error while closing admin: %+v\n", err)
		}
	}()

	consumerGroupsMap, err := newAdmin.ListConsumerGroups()
	die(err)

	consumerGroups := keys(consumerGroupsMap, sorted)

	fmt.Printf("Consumer groups:\n")
	// the consumer group name is the key
	for _, consumerGroup := range consumerGroups {
		fmt.Printf("  %s\n", consumerGroup)
	}
	fmt.Printf("\nTotal: %d\n", len(consumerGroups))
}

// extract all keys of the map, sort the keys if sorted flag is set
func keys(groups map[string]string, sorted *bool) []string {
	keys := make([]string, len(groups))
	for consumerGroup, _ := range groups {
		keys = append(keys, consumerGroup)
	}

	if *sorted {
		sort.Strings(keys)
	}

	return keys
}
