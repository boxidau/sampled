package consumer

import (
	"strings"

	"github.com/boxidau/sampled/consumer/config"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

func NewKafkaConsumer(cfg *config.KafkaConfig) (*kafka.Consumer, error) {
	cfgMap := kafka.ConfigMap{
		"bootstrap.servers":  strings.Join(cfg.BootstrapServers, ","),
		"security.protocol":  cfg.SecurityProtocol,
		"group.id":           cfg.GroupId,
		"auto.offset.reset":  cfg.ResetPosition,
		"enable.auto.commit": false,
	}

	if cfg.Sasl != nil {
		cfgMap["sasl.mechanisms"] = cfg.Sasl.Mechanism
		cfgMap["sasl.username"] = cfg.Sasl.Username
		cfgMap["sasl.password"] = cfg.Sasl.Password
	}

	return kafka.NewConsumer(&cfgMap)
}
