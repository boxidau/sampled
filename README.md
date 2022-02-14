SampleD
---

Realtime event analytics capture and processor

- Emit samples from your application code (libraries provided)
- Configure fluentbit to capture samples on hosts
- Output from fluentbit to kafka
- Consume samples from Kafka in SampleD consumer
- SampleD will manage schema and store samples in ClickHouse/Apache Druid
- Query with Apache Superset


## Consumer
```
go run ./consumer \
    -logtostderr \
    -clickhouse_dsn clickhouse://cliskhouse_username:password@clickhouse.dnsname:9000/sampled \
    -kafka_sasl_username kafka_username \
    -kafka_sasl_password kafka_password \
    -kafka_security_protocol SASL_PLAINTEXT \
    -kafka_sasl_mechanisms SCRAM-SHA-512 \
    -kafka_bootstrap_servers kafka-bootstrap.dnsname:32559 \
    -sample_flush_interval_max 5s
```

## Creating Samples
see example script in `libsampled/python/example`