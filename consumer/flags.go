package main

import (
	"flag"
	"time"
)

// schema loader flags
var FLAG_schemaDirectory = flag.String("schema_file_directory", "schema", "Directory from which to load stream schema")

// clickhouse flags
var FLAG_clickhouseConnectionString = flag.String("clickhouse_dsn", "", "Clickhouse connection string, example: clickhouse://username:password@host1:9000,host2:9000/database")

// kafka flags
var FLAG_kafkaBootstrapServers = flag.String("kafka_bootstrap_servers", "", "Comma separated list of kafka bootstrap servers")
var FLAG_kafkaSASLUsername = flag.String("kafka_sasl_username", "", "kafka SASL username")
var FLAG_kafkaSASLPassword = flag.String("kafka_sasl_password", "", "kafka SASL pasword")
var FLAG_kafkaSASLMech = flag.String("kafka_sasl_mechanisms", "SCRAM-SHA-512", "kafka SASL security mechanism")
var FLAG_kafkaSecurityProtocol = flag.String("kafka_security_protocol", "SASL_PLAINTEXT", "kafka security protocol")
var FLAG_kafkaGroupID = flag.String("kafka_group_id", "sampled__kafka_group", "Consumer group ID to use")
var FLAG_kafkaTopics = flag.String("kafka_topics", "sampled", "comma-separated topic list")

// batching flags
var FLAG_flushInterval = flag.Duration("sample_flush_interval_max", 20*time.Second, "Interval between sample flush to storage")
var FLAG_flushSizeMax = flag.Int("sample_buffer_max_size", 1000, "max number of samples to hold before flushing to storage")
