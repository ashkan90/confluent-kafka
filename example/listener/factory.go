package listener

import (
	"confluent_kafka/example/constants"
	"confluent_kafka/example/event"
	"confluent_kafka/example/eventmanager"
	"errors"
)

type ExampleEventHandlerFactory struct {
	service any
}

func NewExampleEventHandlerFactory(cs any) eventmanager.IEventHandlerFactory {
	return &ExampleEventHandlerFactory{service: cs}
}

func (f *ExampleEventHandlerFactory) Make(e event.Event) (eventmanager.EventHandler, error) {
	switch e.Type() {
	case constants.DocumentItemSavedType:
		return nil, nil

	default:
		return nil, errors.New("şlmlş")
	}
}
