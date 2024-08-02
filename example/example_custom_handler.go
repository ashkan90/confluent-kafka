package main

import (
	"confluent_kafka"
	"confluent_kafka/example/eventmanager"
	"context"
	"github.com/sirupsen/logrus"
)

type customHandler struct {
	logger       *logrus.Logger
	eventManager eventmanager.IEventManager
}

func NewCustomHandler(l *logrus.Logger, em eventmanager.IEventManager) confluent_kafka.CustomHandler {
	return &customHandler{
		logger:       l,
		eventManager: em,
	}
}

func (c *customHandler) Do(ctx context.Context, msg *confluent_kafka.ConsumerMessage) error {
	_, err := c.eventManager.Handle(ctx, msg)

	return err
}
