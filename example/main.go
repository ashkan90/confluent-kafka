package main

import (
	"confluent_kafka"
	"confluent_kafka/example/event"
	eventmanager2 "confluent_kafka/example/eventmanager"
	"context"
	"errors"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/sirupsen/logrus"
	"gitlab.com/letgo-turkey/classifieds/clients/newrelic/nrclient"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// Example event names
const (
	DocumentItemSavedType = "item-saved"
)

type ExampleEventHandlerFactory struct {
	service any
}

func NewExampleEventHandlerFactory(cs any) eventmanager2.IEventHandlerFactory {
	return &ExampleEventHandlerFactory{service: cs}
}

func (f *ExampleEventHandlerFactory) Make(e event.Event) (eventmanager2.EventHandler, error) {
	switch e.Type() {
	case DocumentItemSavedType:
		return nil, nil
	default:
		return nil, errors.New("şlmlş")
	}
}

func main() {
	l := logrus.New()
	ctx := context.Background()
	nrInstance, _ := nrclient.InitNewRelic(nrclient.Config{Enabled: false})
	_customEventHandler := NewExampleEventHandlerFactory(nil)
	_customEventHandlerFactory := event.NewEventFactory()
	_customEventManager := eventmanager2.NewEventManager(_customEventHandler, _customEventHandlerFactory)
	_customHandler := NewCustomHandler(l, _customEventManager)
	_consumerManager := confluent_kafka.NewConsumerManager(l, _customHandler, nrInstance)
	_consumerGroupBridgeConfig := confluent_kafka.ConsumerBridgeConfig{
		ReadTimeout: time.Duration(-1),
		ConfigMap: &kafka.ConfigMap{
			"bootstrap.servers":  "localhost:29092",
			"group.id":           "confluent-kafka-group_id$1",
			"auto.offset.reset":  "earliest",
			"enable.auto.commit": false,
		},
	}
	_consumerGroupBridge, _consumerGroupBridgeErr := confluent_kafka.NewConsumerGroupBridge(l, _consumerGroupBridgeConfig)
	_consumerGroup, _consumerGroupErr := confluent_kafka.NewConsumerGroup(ctx, l, _consumerGroupBridge)
	_consumerGroupHandler := confluent_kafka.NewSyncHandler(l, _consumerManager)
	_consumerInstance := confluent_kafka.NewConsumerInstance(l, _consumerGroup, _consumerGroupHandler)

	log.Println("unhandled errors", _consumerGroupErr, _consumerGroupBridgeErr)

	topics := []string{"my_random_topic"}
	ctx, cancel := context.WithCancel(context.Background())
	wg := new(sync.WaitGroup)
	wg.Add(1)

	go func() {
		defer wg.Done()
		_consumerInstance.Consume(ctx, topics)
	}()

	<-_consumerInstance.Handler().Status()

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-ctx.Done():
		l.Info("terminating: context cancelled")
	case <-sigterm:
		l.Info("terminating: via signal")
	}

	cancel()
	wg.Wait()
}
