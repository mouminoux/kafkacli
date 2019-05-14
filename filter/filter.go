package filter

import (
	"github.com/Shopify/sarama"
)

type Filter func(message *sarama.ConsumerMessage) bool

func Anded(ff []Filter) Filter {
	return func(message *sarama.ConsumerMessage) bool {
		for _, f := range ff {
			if !f(message) {
				return false
			}
		}
		return true
	}
}
