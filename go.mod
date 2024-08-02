module confluent_kafka

go 1.22.3

replace gitlab.com/letgo-turkey/classifieds/clients/newrelic => gitlab.com/letgo-turkey/classifieds/clients/newrelic.git v1.1.0

require (
	github.com/confluentinc/confluent-kafka-go/v2 v2.5.0
	github.com/newrelic/go-agent/v3 v3.33.0
	github.com/sirupsen/logrus v1.9.3
	gitlab.com/letgo-turkey/classifieds/clients/newrelic v1.1.0
	k8s.io/utils v0.0.0-20230726121419-3b25d923346b
)

require (
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/newrelic/go-agent/v3/integrations/nrmongo v1.1.3 // indirect
	github.com/newrelic/go-agent/v3/integrations/nrredis-v9 v1.0.0 // indirect
	github.com/redis/go-redis/v9 v9.5.3 // indirect
	go.mongodb.org/mongo-driver v1.10.2 // indirect
	golang.org/x/net v0.26.0 // indirect
	golang.org/x/sys v0.21.0 // indirect
	golang.org/x/text v0.16.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20240318140521-94a12d6c2237 // indirect
	google.golang.org/grpc v1.62.1 // indirect
	google.golang.org/protobuf v1.33.0 // indirect
)
