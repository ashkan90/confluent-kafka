package kafka

import (
	"confluent_kafka"
	"context"

	"github.com/newrelic/go-agent/v3/newrelic"
	"github.com/sirupsen/logrus"

	"gitlab.com/letgo-turkey/classifieds/clients/newrelic/nrclient"
)

type ConsumerGroupHandler interface {
	confluent_kafka.ConsumerGroupHandler

	Ready()
	Status() chan bool
}

type CustomHandler interface {
	Do(ctx context.Context, msg *confluent_kafka.ConsumerMessage) error
}

type IConsumerManager interface {
	Process(ctx context.Context, msg *confluent_kafka.ConsumerMessage) error
}

type consumerManager struct {
	logger           *logrus.Logger
	customHandler    CustomHandler
	newRelicInstance nrclient.INewRelicInstance
}

func NewConsumerManager(l *logrus.Logger, ch CustomHandler, ni nrclient.INewRelicInstance) IConsumerManager {
	return &consumerManager{
		logger:           l,
		customHandler:    ch,
		newRelicInstance: ni,
	}
}

func (cm *consumerManager) Process(ctx context.Context, msg *confluent_kafka.ConsumerMessage) error {
	txn := cm.newRelicInstance.Application().StartTransaction(string(msg.Key))
	txn.AddAttribute("event.topic", msg.Topic)
	txn.AddAttribute("event.partition", msg.Partition)
	txn.AddAttribute("event.offset", msg.Offset)

	defer txn.End()

	ctx = newrelic.NewContext(ctx, txn)
	if err := cm.customHandler.Do(ctx, msg); err != nil {
		cm.logger.WithField("event", cm.prepareLogFields(msg)).WithError(err).Error("processing error")
	}

	return nil
}

func (cm *consumerManager) prepareLogFields(msg *confluent_kafka.ConsumerMessage) logrus.Fields {
	return logrus.Fields{
		"topic":     msg.Topic,
		"key":       string(msg.Key),
		"partition": msg.Partition,
		"offset":    msg.Offset,
		"body":      string(msg.Value),
	}
}
