package event

import (
	"encoding/json"
	"errors"
)

type Type string

var (
	ErrEventUnexpectedType   = errors.New("event: unexpected event type")
	ErrHandlerUnexpectedType = errors.New("handler: unexpected event type")
)

type Event interface {
	Type() string
	Data() interface{}
}

type IEventFactory interface {
	Make(eventType Type, data []byte) (Event, error)
}

type eventFactory struct{}

func NewEventFactory() IEventFactory {
	return &eventFactory{}
}

func (ef *eventFactory) Make(eventType Type, data []byte) (Event, error) {
	event := ef.getEventByType(eventType)
	if event == nil {
		return nil, ErrEventUnexpectedType
	}

	if err := json.Unmarshal(data, &event); err != nil {
		return nil, err
	}

	return event, nil
}

func (ef *eventFactory) getEventByType(t Type) Event {
	//switch Type(t) {
	//case PaymentCreatedEventType:
	//	return &PaymentCreatedEvent{}
	//case PaymentProvisionReleasedEventType:
	//	return &PaymentProvisionReleasedEvent{}
	//case PaymentRefundCreatedEventType:
	//	return &PaymentRefundCreatedEvent{}
	//case PaymentTransactionCreatedEventType:
	//	return &PaymentTransactionCreatedEvent{}
	//case NotificationCreatedEventType:
	//	return &NotificationCreatedEvent{}
	//case AccountLinkedEventType:
	//	return &AccountLinkedEvent{}
	//case RentCommittedEventType:
	//	return &RentCommittedEvent{}
	//case RentCompletedEventType:
	//	return &RentCompletedEvent{}
	//default:
	//	return nil
	//}
	return nil
}
