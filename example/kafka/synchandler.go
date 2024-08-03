package kafka

import (
	"confluent_kafka"

	"github.com/sirupsen/logrus"
)

type syncHandler struct {
	logger          *logrus.Logger
	consumerManager IConsumerManager

	ready chan bool
}

func NewSyncHandler(l *logrus.Logger, cm IConsumerManager) ConsumerGroupHandler {
	return &syncHandler{
		logger:          l,
		consumerManager: cm,
		ready:           make(chan bool),
	}
}

func (h *syncHandler) Setup(_ confluent_kafka.ConsumerGroup) error {
	close(h.ready)
	return nil
}

func (h *syncHandler) Cleanup(_ confluent_kafka.ConsumerGroup) error {
	return nil
}

func (h *syncHandler) ConsumeClaim(sess confluent_kafka.ConsumerGroup, claim confluent_kafka.ConsumerGroupClaim) error {
	ch := make(chan error)

	for {
		select {
		case <-sess.Context().Done():
			return nil
		case msg := <-claim.Messages():
			if msg == nil || sess.Context().Err() != nil {
				return nil
			}

			go func() {
				ch <- h.consumerManager.Process(sess.Context(), msg)
			}()

			select {
			case <-sess.Context().Done():
				//sess.MarkMessage(msg, "")
				return nil
			case <-ch:
				//sess.MarkMessage(msg, "")
			}
		}
	}
}

func (h *syncHandler) Ready() {
	h.ready = make(chan bool)
}

func (h *syncHandler) Status() chan bool {
	return h.ready
}
