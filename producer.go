package confluent_kafka

import "github.com/confluentinc/confluent-kafka-go/v2/kafka"

type Output struct {
	Partition int32
	Offset    int64
}

type IProducerInstance interface {
	SendMessage(mi *MessageInput) (*Output, error)
	PrepareMessage(mi *MessageInput) *kafka.Message
	Close() error
}

type producerInstance struct {
	producer *kafka.Producer
}

func (p *producerInstance) SendMessage(mi *MessageInput) (*Output, error) {
	//TODO implement me
	panic("implement me")
}

func (p *producerInstance) PrepareMessage(mi *MessageInput) *kafka.Message {
	if mi.Partition == 0 {
		mi.Partition = kafka.PartitionAny
	}

	return &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &mi.Topic, Partition: mi.Partition},
		Key:            []byte(mi.Key),
		Value:          []byte(mi.Value),
	}
}

func (p *producerInstance) Close() error {
	//TODO implement me
	panic("implement me")
}

func InitProducer(config *kafka.ConfigMap) (IProducerInstance, error) {
	p, err := kafka.NewProducer(config)
	if err != nil {
		return nil, err
	}

	return &producerInstance{producer: p}, nil
}
