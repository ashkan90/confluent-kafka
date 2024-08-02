package confluent_kafka

type MessageInput struct {
	Topic     string
	Partition int32
	Key       string
	Value     string
	Attribute *MessageAttribute
}

type MessageAttribute struct {
	Type      string
	Attempts  int
	CreatedAt int64
}
