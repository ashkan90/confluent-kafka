package confluent_kafka

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/sirupsen/logrus"
	"k8s.io/utils/strings/slices"
)

// ErrClosedConsumerGroup is the error returned when a method is called on a consumer group that has been closed.
var ErrClosedConsumerGroup = errors.New("kafka: tried to use a consumer group that was closed")

var (
	InvalidTopicName = "value will replaced at runtime"
	InvalidOffset    = int64(-1)
	InvalidPartition = int32(-1)
)

type none struct{}

type ConsumerGroup interface {
	// Consume joins a cluster of consumers for a given list of topics and
	// starts a blocking ConsumerGroupSession through the ConsumerGroupHandler.
	//
	// Please note, that once a rebalance is triggered, sessions must be completed within
	// Config.Consumer.Group.Rebalance.Timeout. This means that ConsumeClaim() functions must exit
	// as quickly as possible to allow time for Cleanup() and the final offset commit. If the timeout
	// is exceeded, the consumer will be removed from the group by Kafka, which will cause offset
	// commit failures.
	// This method should be called inside an infinite loop, when a
	// server-side rebalance happens, the consumer session will need to be
	// recreated to get the new claims.
	Consume(topics []string, handler ConsumerGroupHandler) error

	Context() context.Context

	// Errors returns a read channel of errors that occurred during the consumer life-cycle.
	// By default, errors are logged and not returned over this channel.
	// If you want to implement any custom error handling, set your config's
	// Consumer.Return.Errors setting to true, and read from this channel.
	Errors() <-chan error

	// Close stops the ConsumerGroup and detaches any running sessions. It is required to call
	// this function before the object passes out of scope, as it will otherwise leak memory.
	Close() error
}

type ConsumerGroupHandler interface {
	// Setup is run at the beginning of a new session, before ConsumeClaim.
	Setup(ConsumerGroup) error

	// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
	// but before the offsets are committed for the very last time.
	Cleanup(ConsumerGroup) error

	// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
	// Once the Messages() channel is closed, the Handler must finish its processing
	// loop and exit.
	ConsumeClaim(ConsumerGroup, ConsumerGroupClaim) error
}

type consumerGroup struct {
	logger         *logrus.Logger
	consumerBridge ConsumerGroupBridge

	errors chan error

	ctx    context.Context
	cancel func()

	waitGroup   sync.WaitGroup
	releaseOnce sync.Once

	lock       sync.Mutex
	errorsLock sync.RWMutex
	closed     chan none
	closeOnce  sync.Once
}

type ConsumerConfig struct {
	// ReadTimeout
	ReadTimeout time.Duration
	// Rebalance
	Rebalance kafka.RebalanceCb
	// ConfigMap
	ConfigMap *kafka.ConfigMap
}

func NewConsumerGroup(ctx context.Context, l *logrus.Logger, cgb ConsumerGroupBridge) (ConsumerGroup, error) {
	ctx, cancel := context.WithCancel(ctx)
	return &consumerGroup{
		logger:         l,
		consumerBridge: cgb,
		errors:         make(chan error),
		ctx:            ctx,
		cancel:         cancel,
		closed:         make(chan none),
	}, nil
}

func (c *consumerGroup) Consume(topics []string, handler ConsumerGroupHandler) error {
	for _, topic := range topics {
		c.waitGroup.Add(1)
		go func(_topic string, _handler ConsumerGroupHandler) {
			defer c.waitGroup.Done()
			//defer c.cancel()

			c.consume(_topic, _handler)
		}(topic, handler)
	}

	return c.release()
}

func (c *consumerGroup) Errors() <-chan error {
	return c.errors
}

func (c *consumerGroup) Close() (err error) {
	c.closeOnce.Do(func() {
		close(c.closed)

		err = c.consumerBridge.Client().Unsubscribe()

		go func() {
			c.errorsLock.Lock()
			defer c.errorsLock.Unlock()
			close(c.errors)
		}()

		for e := range c.errors {
			err = e
		}

		if e := c.consumerBridge.Client().Close(); e != nil {
			err = e
		}

		err = c.release()
	})

	return
}

func (c *consumerGroup) Context() context.Context {
	return c.ctx
}

type interchangeMessage struct {
	message *kafka.Message
	err     error
}

func (c *consumerGroup) consume(topic string, handler ConsumerGroupHandler) {
	if c.consumerBridge.Client().IsClosed() {
		return
	}

	topics, err := c.consumerBridge.Client().Subscription()
	if err != nil {
		return
	}

	if !slices.Contains(topics, topic) {
		c.consumerBridge.Subscribe(topic)
	}

	interchangeChannel := make(chan *interchangeMessage)

	go func() {
		for {
			msg, err := c.consumerBridge.Read()
			interchangeChannel <- &interchangeMessage{
				message: msg,
				err:     err,
			}
		}
	}()

	var (
		partition = InvalidPartition
		offset    = InvalidOffset
		msg       *interchangeMessage
	)

	// first-time partitionClaim initiation
	pcm := newConsumerPartitionClaim(topic, partition, offset)

	for {
		select {
		case msg = <-interchangeChannel:
			if msg.err != nil {
				c.handleError(err, topic, partition)
			}

			c.lock.Lock()
			partition = msg.message.TopicPartition.Partition
			offset = int64(msg.message.TopicPartition.Offset)
			c.lock.Unlock()

			pcm.messages <- &ConsumerMessage{
				Timestamp: time.Now(),
				Key:       msg.message.Key,
				Value:     msg.message.Value,
				Topic:     topic,
				Partition: partition,
				Offset:    offset,
			}

			claim := newConsumerGroupClaim(topic, partition, offset, pcm)

			// handle errors
			go func() {
				for err := range pcm.Errors() {
					c.handleError(err, topic, partition)
				}
			}()

			// close when consuming-session ends
			go func() {
				select {
				case <-c.ctx.Done():
				case <-c.closed:
				}
				pcm.AsyncClose()
			}()

			// start processing
			go func() {
				if err := handler.ConsumeClaim(c, claim); err != nil {
					c.handleError(err, topic, partition)
				}
			}()

			select {
			case <-c.closed:
				// ensure consumer is closed & drained
				pcm.AsyncClose()
				for _, err := range claim.waitClosed() {
					c.handleError(err, topic, partition)
				}
				return
			default:
			}

		}
	}
}

func (c *consumerGroup) release() (err error) {
	// signal release, stop heartbeat
	//c.cancel()

	// wait for consumers to exit
	c.waitGroup.Wait()
	return
}

func (c *consumerGroup) handleError(err error, topic string, partition int32) {
	var consumerError *ConsumerError
	if ok := errors.As(err, &consumerError); !ok && topic != "" && partition > -1 {
		err = &ConsumerError{
			Topic:     topic,
			Partition: partition,
			Err:       err,
		}
	}

	c.errorsLock.RLock()
	defer c.errorsLock.RUnlock()
	select {
	case <-c.closed:
		// consumer is closed
		return
	default:
	}

	select {
	case c.errors <- err:
	default:
		// no error listener
	}
}

type ConsumerGroupClaim interface {
	// Topic returns the consumed topic name.
	Topic() string

	// Partition returns the consumed partition.
	Partition() int32

	// InitialOffset returns the initial offset that was used as a starting point for this claim.
	InitialOffset() int64

	// Messages returns the read channel for the messages that are returned by
	// the broker. The messages channel will be closed when a new rebalance cycle
	// is due. You must finish processing and mark offsets within
	// Config.Consumer.Group.Session.Timeout before the topic/partition is eventually
	// re-assigned to another group member.
	Messages() <-chan *ConsumerMessage
}

type consumerGroupClaim struct {
	topic     string
	partition int32
	offset    int64
	PartitionConsumer
}

func newConsumerGroupClaim(topic string, partition int32, offset int64, pcm PartitionConsumer) *consumerGroupClaim {
	return &consumerGroupClaim{
		topic:             topic,
		partition:         partition,
		offset:            offset,
		PartitionConsumer: pcm,
	}
}

func (c *consumerGroupClaim) Topic() string {
	return c.topic
}

func (c *consumerGroupClaim) Partition() int32 {
	return c.partition
}

func (c *consumerGroupClaim) InitialOffset() int64 {
	return c.offset
}

// Drains messages and errors, ensures the claim is fully closed.
func (c *consumerGroupClaim) waitClosed() (errs ConsumerErrors) {
	go func() {
		for range c.Messages() {
		}
	}()

	for err := range c.Errors() {
		errs = append(errs, err)
	}
	return
}
