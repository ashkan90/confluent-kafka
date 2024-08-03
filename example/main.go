package main

import (
	"confluent_kafka"
	"confluent_kafka/example/event"
	"confluent_kafka/example/eventmanager"
	kafka_usecase "confluent_kafka/example/kafka"
	"confluent_kafka/example/listener"
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/sirupsen/logrus"
	"gitlab.com/letgo-turkey/classifieds/clients/newrelic/nrclient"
)

func main() {
	l := logrus.New()
	ctx := context.Background()
	nrInstance, _ := nrclient.InitNewRelic(nrclient.Config{Enabled: false})
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

	_consumer, _consumerErr := confluent_kafka.NewConsumerGroup(ctx, l, _consumerGroupBridge)
	eventHandlerFactory := listener.NewExampleEventHandlerFactory(nil)
	eventFactory := event.NewEventFactory()
	eventManager := eventmanager.NewEventManager(eventHandlerFactory, eventFactory)
	customHandler := listener.NewCustomHandler(l, eventManager)
	consumerManager := kafka_usecase.NewConsumerManager(l, customHandler, nrInstance)
	consumerGroup := kafka_usecase.NewSyncHandler(l, consumerManager)
	consumerInstance := kafka_usecase.NewConsumerInstance(l, _consumer, consumerGroup)

	log.Println("unhandled errors", _consumerErr, _consumerGroupBridgeErr)

	topics := []string{"my_random_topic"}
	ctx, cancel := context.WithCancel(context.Background())
	wg := new(sync.WaitGroup)
	wg.Add(1)

	go func() {
		defer wg.Done()
		consumerInstance.Consume(ctx, topics)
	}()

	<-consumerInstance.Handler().Status()

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
