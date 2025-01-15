package main

import (
	"fmt"
	"github.com/IBM/sarama"
	cli "github.com/jawher/mow.cli"
	"log"
	"regexp"
	"sort"
	"strings"
)

func consumerGroupsCmd(c *cli.Cmd) {
	// add unique & sorted flags
	var (
		sorted      = c.BoolOpt("s sorted", false, "sort the consumer groups (ascending)")
		listOffsets = c.BoolOpt("l list-offsets", false, "list the offsets for each consumer groups")
		filter      = c.StringOpt("f filter", "", "filter the consumer groups (regex)")
		topics      = c.StringsOpt("t topics", nil, "topic(s) to consume from")
	)

	c.Action = func() {
		cfg := config(*useSSL, *sslCAFile, *sslCertFile, *sslKeyFile)
		consumerGroups(cfg, splitFlatten(*bootstrapServers), sorted, listOffsets, filter, topics)
	}
}

func consumerGroups(config *sarama.Config, bootstrapServers []string, sorted *bool, listOffsets *bool, filter *string, topicsFilter *[]string) {
	if topicsFilter != nil {
		fmt.Printf("Listing consumer groups for topics %q on broker(s) %q\n", strings.Join(*topicsFilter, ", "), strings.Join(bootstrapServers, ", "))
	} else {
		fmt.Printf("Listing consumer groups for all topics on broker(s) %q\n", strings.Join(bootstrapServers, ", "))
	}

	newAdmin, err := sarama.NewClusterAdmin(bootstrapServers, config)
	die(err)

	defer func() {
		if err := newAdmin.Close(); err != nil {
			log.Printf("error while closing admin: %+v\n", err)
		}
	}()

	topicPartitions := make(map[string][]int32)
	if *listOffsets {
		topics, err := newAdmin.ListTopics()
		die(err)

		// convert topics to map[string][]int32{} (topic -> partitions)
		for topicName, topicDetail := range topics {
			if topics != nil && !arrayContains(topicsFilter, topicName) {
				continue
			}
			numPartitions := int(topicDetail.NumPartitions)
			partitions := make([]int32, numPartitions)
			for i := 0; i < numPartitions; i++ {
				partitions[i] = int32(i)
			}
			topicPartitions[topicName] = partitions
		}
	}

	consumerGroupsMap, err := newAdmin.ListConsumerGroups()
	die(err)

	consumerGroups := keys(consumerGroupsMap, sorted)
	if *filter != "" {
		consumerGroups = filterSlice(consumerGroups, *filter)
	}

	fmt.Printf("Consumer groups:\n")
	for _, consumerGroup := range consumerGroups {
		logged := false

		if *listOffsets {
			offsets, err := newAdmin.ListConsumerGroupOffsets(consumerGroup, topicPartitions)
			die(err)

			// print only if any partition offset is not -1
			for topic, partitions := range offsets.Blocks {
				if topicsFilter != nil && !arrayContains(topicsFilter, topic) {
					continue
				}
				// if all partitions are -1, skip this topic
				partitionsWithOffsets := len(partitions)
				for _, offset := range partitions {
					if offset.Offset == -1 {
						partitionsWithOffsets--
					}
				}
				if partitionsWithOffsets == 0 {
					// no partition with offset, skip this topic
					continue
				}

				if !logged {
					fmt.Printf("  %s\n", consumerGroup)
					logged = true
				}
				fmt.Printf("    Topic %s:\n", topic)
				for partition, offset := range partitions {
					fmt.Printf("      - partition %d: %d\n", partition, offset.Offset)
				}
			}
		}
	}
	fmt.Printf("\nTotal: %d\n", len(consumerGroups))
}

func arrayContains(topics *[]string, topic string) bool {
	if len(*topics) == 0 {
		return false
	}
	for _, t := range *topics {
		if t == topic {
			return true
		}
	}
	return false
}

func filterSlice(groups []string, filterRegex string) []string {
	var filtered []string
	for _, consumerGroup := range groups {
		if match, _ := regexp.MatchString(filterRegex, consumerGroup); match {
			filtered = append(filtered, consumerGroup)
		}
	}
	return filtered
}

// extract all keys of the map, sort the keys if sorted flag is set
func keys(groups map[string]string, sorted *bool) []string {
	var keys []string
	for consumerGroup, _ := range groups {
		keys = append(keys, consumerGroup)
	}

	if *sorted {
		sort.Strings(keys)
	}

	return keys
}
