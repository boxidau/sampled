package config

import (
	"io/ioutil"
	"os"
	"time"

	"github.com/pkg/errors"
	"gopkg.in/yaml.v2"
)

type ClickHouseConfig struct {
	Hosts    []string `yaml:"hosts"`
	Username string   `yaml:"username"`
	Password string   `yaml:"password"`
	Database string   `yaml:"database"`
}

type KafkaSaslConfig struct {
	Username  string `yaml:"username"`
	Password  string `yaml:"password"`
	Mechanism string `yaml:"mechanism"`
}

type KafkaConfig struct {
	GroupId          string           `yaml:"groupId"`
	ResetPosition    string           `yaml:"resetPosition"`
	SecurityProtocol string           `yaml:"securityProtocol"`
	BootstrapServers []string         `yaml:"bootstrapServers"`
	Topics           []string         `yaml:"topics"`
	Sasl             *KafkaSaslConfig `yaml:"sasl,omitempty"`
}

type StoreConfig struct {
	ClickHouse ClickHouseConfig `yaml:"clickhouse"`
}

type TuningConfig struct {
	FlushInterval    time.Duration `yaml:"flushInterval"`
	SampleBufferSize int64         `yaml:"sampleBufferSize"`
}

type SampledConfig struct {
	Store  StoreConfig  `yaml:"store"`
	Kafka  KafkaConfig  `yaml:"kafka"`
	Tuning TuningConfig `yaml:"tuning"`
}

func LoadConfig(filePath string) (*SampledConfig, error) {
	rawConfig, err := ioutil.ReadFile(filePath)
	if err != nil {
		return nil, errors.Wrap(err, "unable to load config file")
	}

	processedConfig := []byte(os.ExpandEnv(string(rawConfig)))
	conf := &SampledConfig{}
	if err := yaml.Unmarshal(processedConfig, conf); err != nil {
		return nil, err
	}

	// set kafka defaults
	if conf.Kafka.ResetPosition != "latest" && conf.Kafka.ResetPosition != "earliest" {
		conf.Kafka.ResetPosition = "latest"
	}
	if conf.Kafka.GroupId == "" {
		conf.Kafka.GroupId = "sampled_kafka_group"
	}
	if len(conf.Kafka.Topics) == 0 {
		conf.Kafka.Topics = []string{"sampled"}
	}

	//set tuning defaults
	if conf.Tuning.FlushInterval == 0 {
		conf.Tuning.FlushInterval = 20 * time.Second
	}
	if conf.Tuning.SampleBufferSize == 0 {
		conf.Tuning.SampleBufferSize = 1000
	}

	return conf, nil
}
