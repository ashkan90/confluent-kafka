package eventmanager

import (
	"confluent_kafka/example/event"
	"context"
)

type EventHandler interface {
	Handle(ctx context.Context) error
}

type IEventHandlerFactory interface {
	Make(e event.Event) (EventHandler, error)
}
