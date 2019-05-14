package filter

import (
	"github.com/Shopify/sarama"
)

func Header(key, value string) Filter {
	return func(message *sarama.ConsumerMessage) bool {
		for _, h := range message.Headers {
			if string(h.Key) != key {
				continue
			}
			return string(h.Value) == value
		}
		return false
	}
}
