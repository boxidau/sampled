package main

import (
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

func NewKafkaConsumer() (*kafka.Consumer, error) {
	return kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  *FLAG_kafkaBootstrapServers,
		"sasl.mechanisms":    *FLAG_kafkaSASLMech,
		"security.protocol":  *FLAG_kafkaSecurityProtocol,
		"sasl.username":      *FLAG_kafkaSASLUsername,
		"sasl.password":      *FLAG_kafkaSASLPassword,
		"group.id":           *FLAG_kafkaGroupID,
		"enable.auto.commit": false,
		"auto.offset.reset":  "earliest",
	})
}
