package confluent_kafka

import (
	"context"
	"errors"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/sirupsen/logrus"
	"time"
)

type ConsumerGroupBridge interface {
	// SubscribeTopics subscribes to the given topics
	SubscribeTopics(ctx context.Context, topics []string)

	Subscribe(topic string)

	// Read reads message from subscribed topics
	Read() (*kafka.Message, error)

	// Client grants full-access to kafka
	Client() *kafka.Consumer
}

type consumerGroupBridge struct {
	logger   *logrus.Logger
	consumer *kafka.Consumer

	config ConsumerBridgeConfig
}

type ConsumerBridgeConfig struct {
	// ReadTimeout
	ReadTimeout time.Duration
	// Rebalance
	Rebalance kafka.RebalanceCb
	// ConfigMap
	ConfigMap *kafka.ConfigMap
}

func NewConsumerGroupBridge(l *logrus.Logger, c ConsumerBridgeConfig) (ConsumerGroupBridge, error) {
	_consumer, err := kafka.NewConsumer(c.ConfigMap)
	if err != nil {
		return nil, err
	}

	return &consumerGroupBridge{
		logger:   l,
		consumer: _consumer,
		config:   c,
	}, nil
}

func (c *consumerGroupBridge) SubscribeTopics(ctx context.Context, topics []string) {
	if len(topics) == 0 {
		_, cancel := context.WithCancelCause(ctx)
		cancel(errors.New("no topics provided to subscribe"))
	}

	subErr := c.consumer.SubscribeTopics(topics, c.config.Rebalance)
	if subErr != nil {
		c.logger.WithContext(ctx).WithError(subErr).Error("error subscribing to topics")
		_, cancel := context.WithCancelCause(ctx)
		cancel(subErr)
		return
	}
}

func (c *consumerGroupBridge) Subscribe(topic string) {
	subErr := c.consumer.Subscribe(topic, c.config.Rebalance)
	if subErr != nil {
		c.logger.WithError(subErr).Error("error subscribing to topics")
		return
	}
}

func (c *consumerGroupBridge) Read() (*kafka.Message, error) {
	message, readErr := c.consumer.ReadMessage(c.config.ReadTimeout)
	if readErr != nil {
		return nil, readErr
	}

	return message, nil
}

func (c *consumerGroupBridge) Client() *kafka.Consumer {
	return c.consumer
}
