package confluent_kafka

import (
	"context"
	"fmt"
	"github.com/sirupsen/logrus"
	"sync"
	"time"
)

// ConsumerMessage encapsulates a Kafka message returned by the consumer.
type ConsumerMessage struct {
	Headers        []*RecordHeader // only set if kafka is version 0.11+
	Timestamp      time.Time       // only set if kafka is version 0.10+, inner message timestamp
	BlockTimestamp time.Time       // only set if kafka is version 0.10+, outer (compressed) block timestamp

	Key, Value []byte
	Topic      string
	Partition  int32
	Offset     int64
}

type ConsumerErrors []*ConsumerError

func (ce ConsumerErrors) Error() string {
	return fmt.Sprintf("kafka: %d errors while consuming", len(ce))
}

// ConsumerError is what is provided to the user when an error occurs.
// It wraps an error and includes the topic and partition.
type ConsumerError struct {
	Topic     string
	Partition int32
	Err       error
}

func (ce ConsumerError) Error() string {
	return fmt.Sprintf("kafka: error while consuming %s/%d: %s", ce.Topic, ce.Partition, ce.Err)
}
func (ce ConsumerError) Unwrap() error {
	return ce.Err
}

type ConsumerInstance interface {
	Handler() ConsumerGroupHandler
	Consume(ctx context.Context, topics []string)

	Close()
}

type consumerInstance struct {
	logger   *logrus.Logger
	consumer ConsumerGroup
	handler  ConsumerGroupHandler

	config ConsumerConfig
}

func NewConsumerInstance(l *logrus.Logger, c ConsumerGroup, h ConsumerGroupHandler) ConsumerInstance {
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

type PartitionConsumer interface {
	// AsyncClose initiates a shutdown of the PartitionConsumer. This method will return immediately, after which you
	// should continue to service the 'Messages' and 'Errors' channels until they are empty. It is required to call this
	// function, or Close before a consumer object passes out of scope, as it will otherwise leak memory. You must call
	// this before calling Close on the underlying client.
	AsyncClose()

	// Close stops the PartitionConsumer from fetching messages. It will initiate a shutdown just like AsyncClose, drain
	// the Messages channel, harvest any errors & return them to the caller. Note that if you are continuing to service
	// the Messages channel when this function is called, you will be competing with Close for messages; consider
	// calling AsyncClose, instead. It is required to call this function (or AsyncClose) before a consumer object passes
	// out of scope, as it will otherwise leak memory. You must call this before calling Close on the underlying client.
	Close() error

	// Messages returns the read channel for the messages that are returned by
	// the broker.
	Messages() <-chan *ConsumerMessage

	// Errors returns a read channel of errors that occurred during consuming, if
	// enabled. By default, errors are logged and not returned over this channel.
	// If you want to implement any custom error handling, set your config's
	// Consumer.Return.Errors setting to true, and read from this channel.
	Errors() <-chan *ConsumerError
}

type partitionConsumer struct {
	topic     string
	partition int32
	offset    int64

	trigger, dying chan none
	closeOnce      sync.Once
	messages       chan *ConsumerMessage
	errors         chan *ConsumerError
}

func newConsumerPartitionClaim(topic string, partition int32, offset int64) *partitionConsumer {
	return &partitionConsumer{
		topic:     topic,
		partition: partition,
		offset:    offset,
		trigger:   make(chan none, 1),
		dying:     make(chan none),
		closeOnce: sync.Once{},
		messages:  make(chan *ConsumerMessage, 1024),
		errors:    make(chan *ConsumerError, 1024),
	}
}

func (child *partitionConsumer) sendError(err error) {
	child.errors <- &ConsumerError{
		Topic:     child.topic,
		Partition: child.partition,
		Err:       err,
	}
}

func (child *partitionConsumer) Messages() <-chan *ConsumerMessage {
	return child.messages
}

func (child *partitionConsumer) Errors() <-chan *ConsumerError {
	return child.errors
}

func (child *partitionConsumer) AsyncClose() {
	// this triggers whatever broker owns this child to abandon it and close its trigger channel, which causes
	// the dispatcher to exit its loop, which removes it from the consumer then closes its 'messages' and
	// 'errors' channel (alternatively, if the child is already at the dispatcher for some reason, that will
	// also just close itself)
	child.closeOnce.Do(func() {
		close(child.dying)
	})
}

func (child *partitionConsumer) Close() error {
	child.AsyncClose()

	var consumerErrors ConsumerErrors
	for err := range child.errors {
		consumerErrors = append(consumerErrors, err)
	}

	if len(consumerErrors) > 0 {
		return consumerErrors
	}
	return nil
}
