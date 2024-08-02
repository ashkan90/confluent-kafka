package eventmanager

import (
	"confluent_kafka"
	"confluent_kafka/example/event"
	"context"
	"fmt"
	"log"
)

type IEventManager interface {
	Handle(ctx context.Context, msg *confluent_kafka.ConsumerMessage) (*confluent_kafka.MessageInput, error)
	HandleException(eventType string, err error) error
}

type eventManager struct {
	handlerFactory IEventHandlerFactory
	eventFactory   event.IEventFactory
}

func NewEventManager(hFactory IEventHandlerFactory, tFactory event.IEventFactory) IEventManager {
	return &eventManager{
		handlerFactory: hFactory,
		eventFactory:   tFactory,
	}
}

func (em *eventManager) Handle(ctx context.Context, msg *confluent_kafka.ConsumerMessage) (*confluent_kafka.MessageInput, error) {
	//attr := kafka.GetEventAttribute(msg.Headers)
	//e, err := em.eventFactory.Make(attr.Type, msg.Value)
	//if err != nil {
	//	return nil, em.HandleException(attr.Type, err)
	//}
	//
	//eh, ehErr := em.handlerFactory.Make(e)
	//if ehErr != nil {
	//	return nil, em.HandleException(attr.Type, ehErr)
	//}
	//
	//input := &kafka.MessageInput{
	//	Topic:     msg.Topic,
	//	Key:       string(msg.Key),
	//	Value:     string(msg.Value),
	//	Attribute: attr,
	//}

	//if handleErr := eh.Handle(ctx); handleErr != nil {
	//	return input, em.HandleException(attr.Type, handleErr)
	//}
	//
	//return input, nil

	log.Println("Handle()", string(msg.Key), string(msg.Value))
	return nil, nil
}

func (em *eventManager) HandleException(eventType string, err error) error {
	if err == event.ErrEventUnexpectedType {
		return nil
	}

	return fmt.Errorf("event type: %s - err: %v", eventType, err)
}
