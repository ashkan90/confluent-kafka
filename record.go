package confluent_kafka

// RecordHeader stores key and value for a record header
type RecordHeader struct {
	Key   []byte
	Value []byte
}
