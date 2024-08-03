package kafka

import (
	"confluent_kafka"
	"context"

	"github.com/sirupsen/logrus"
)

type ConsumerInstance interface {
	Handler() ConsumerGroupHandler
	Consume(ctx context.Context, topics []string)

	Close()
}

type consumerInstance struct {
	logger   *logrus.Logger
	consumer confluent_kafka.ConsumerGroup
	handler  ConsumerGroupHandler

	config confluent_kafka.ConsumerConfig
}

func NewConsumerInstance(l *logrus.Logger, c confluent_kafka.ConsumerGroup, h ConsumerGroupHandler) ConsumerInstance {
	return &consumerInstance{
		logger:   l,
		handler:  h,
		consumer: c,
	}
}

func (k *consumerInstance) Handler() ConsumerGroupHandler {
	return k.handler
}

func (k *consumerInstance) Close() {
	_ = k.consumer.Close()
}

// Consume subscribes to the provided list of topics in config
// while subscribing to topics rebalanceCb can be used from config
// starts to listen messages immediately
func (k *consumerInstance) Consume(ctx context.Context, topics []string) {
	for {
		if err := k.consumer.Consume(topics, k.handler); err != nil {
			ctx = context.WithValue(ctx, "error", err)
			k.logger.Fatalf("consume: %v", err)
		}

		if ctx.Err() != nil {
			return
		}
	}
}
