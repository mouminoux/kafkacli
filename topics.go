package main

import (
	"fmt"
	"github.com/IBM/sarama"
	"github.com/docker/go-units"
	cli "github.com/jawher/mow.cli"
	"sort"
	"strings"
)

func topicsCmd(c *cli.Cmd) {
	c.Command("list", "list topics", listCmd)
}

func listCmd(cmd *cli.Cmd) {
	var (
		details = cmd.BoolOpt("d details", false, "print topics details")
		topic   = cmd.StringOpt("t topic", "", "topic to list")
		sorted  = cmd.BoolOpt("s sorted", false, "sort the topics by size (descending)")
	)

	cmd.Action = func() {
		cfg := config(*useSSL, *sslCAFile, *sslCertFile, *sslKeyFile)
		list(cfg, splitFlatten(*bootstrapServers), details, topic, sorted)
	}
}

func list(cfg *sarama.Config, bootstrapServers []string, details *bool, queriedTopic *string, sorted *bool) {
	fmt.Printf("Listing topics with size on broker(s) %q\n", strings.Join(bootstrapServers, ", "))

	clusterAdmin, err := sarama.NewClusterAdmin(bootstrapServers, cfg)
	die(err)

	topicsDetails, err := clusterAdmin.ListTopics()
	die(err)

	topics := getTopics(topicsDetails)

	brokers, _, _ := clusterAdmin.DescribeCluster()
	for _, broker := range brokers {
		die(broker.Open(cfg))
	}

	totalSizes := map[string]int64{}

	fmt.Printf("Topics:\n")
	for _, topic := range topics {
		if queriedTopic != nil && *queriedTopic != "" && topic != *queriedTopic {
			continue
		}
		if *details {

			metadata, err := clusterAdmin.DescribeTopics([]string{topic})
			die(err)

			partitions := make([]int32, len(metadata[0].Partitions))
			for i, p := range metadata[0].Partitions {
				partitions[i] = p.ID
			}

			request := &sarama.DescribeLogDirsRequest{
				Version: 0,
				DescribeTopics: []sarama.DescribeLogDirsRequestTopic{
					{
						Topic:        topic,
						PartitionIDs: partitions,
					},
				},
			}

			sizeByPartition := make(map[int32]int64)
			for _, broker := range brokers {
				resp, err := broker.DescribeLogDirs(request)
				die(err)

				for _, t := range resp.LogDirs[0].Topics {
					if t.Topic != topic {
						continue
					}

					for _, partition := range t.Partitions {
						if _, ok := sizeByPartition[partition.PartitionID]; ok {
							continue
						}
						sizeByPartition[partition.PartitionID] = partition.Size
					}
				}
			}

			totalSizes[topic] = 0

			for _, size := range sizeByPartition {
				totalSizes[topic] += size
			}
		}
	}

	if *sorted {
		sort.SliceStable(topics, func(i, j int) bool {
			return totalSizes[topics[i]] > totalSizes[topics[j]]
		})
	}

	for _, topic := range topics {
		totalSize := totalSizes[topic]
		fmt.Printf("  %s: %v\n", topic, units.HumanSize(float64(totalSize)))
	}
}

func getTopics(topicsDetail map[string]sarama.TopicDetail) []string {
	var keys []string
	for topicDetail, _ := range topicsDetail {
		keys = append(keys, topicDetail)
	}

	return keys
}
