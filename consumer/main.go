package main

import (
	"context"
	"encoding/json"
	"flag"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/boxidau/sampled/consumer/sample"
	"github.com/boxidau/sampled/consumer/storedriver"
	"github.com/golang/glog"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

func init() {
	flag.Parse()
}

type Consumer struct {
	StoreDriver   storedriver.StoreDriver
	KafkaConsumer *kafka.Consumer
	lastFlush     time.Time
	sampleBuffer  []sample.Sample
	run           bool
}

func (c *Consumer) flushRequired() bool {
	if len(c.sampleBuffer) >= *FLAG_flushSizeMax {
		return true
	}

	if len(c.sampleBuffer) > 0 && time.Since(c.lastFlush) > *FLAG_flushInterval {
		return true
	}
	return false
}

func (c *Consumer) flush(ctx context.Context) error {
	curBatchSize := len(c.sampleBuffer)
	if curBatchSize == 0 {
		return nil
	}

	glog.Infof("Flushing batch of %d samples to storage", curBatchSize)
	err := c.StoreDriver.InsertSamples(ctx, c.sampleBuffer)
	if err != nil {
		return err
	}
	c.sampleBuffer = make([]sample.Sample, 0, *FLAG_flushSizeMax)
	c.lastFlush = time.Now()
	return nil
}

func (c *Consumer) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	c.flush(ctx)
	glog.Infof("Closing kafka consumer...")
	c.KafkaConsumer.Close()
	glog.Infof("Closing store driver...")
	c.StoreDriver.Close()
}

func main() {
	ctx, cancelFn := context.WithCancel(context.Background())
	defer cancelFn()

	consumer := Consumer{
		run:          true,
		sampleBuffer: make([]sample.Sample, 0, *FLAG_flushSizeMax),
	}
	defer consumer.Close()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		glog.Info("Shutting down")
		consumer.run = false
	}()

	if *FLAG_clickhouseConnectionString == "" {
		glog.Fatal("flag clickhouse_dsn is required")
	}

	chd, err := storedriver.NewClickHouseStoreDriver(*FLAG_clickhouseConnectionString)
	if err != nil {
		glog.Fatalf("Failed to setup clickhouse driver %v", err)
	}
	consumer.StoreDriver = chd

	kafkaConsumer, err := NewKafkaConsumer()
	if err != nil {
		glog.Fatalf("Failed to setup kafka consumer: %v", err)
	}
	consumer.KafkaConsumer = kafkaConsumer

	glog.Infof("Subscribing to kafka topics: %s", *FLAG_kafkaTopics)
	consumer.KafkaConsumer.SubscribeTopics(strings.Split(*FLAG_kafkaTopics, ","), nil)

	glog.Info("Starting sampled consumer")

	for consumer.run {
		ev := consumer.KafkaConsumer.Poll(10)
		switch e := ev.(type) {
		case *kafka.Message:
			var s sample.RawSample
			err = json.Unmarshal(e.Value, &s)
			if err != nil {
				glog.Errorf("Unable to unmarshal sample json")
			} else {
				smp, err := sample.SampleFromRawSample(&s)
				if err != nil {
					glog.Errorf("Skipping invalid sample: %v", err)
				}
				consumer.sampleBuffer = append(consumer.sampleBuffer, smp)
			}
		case kafka.PartitionEOF:
			glog.Infof("Kafka partition reached EOF %v", e)
		case kafka.Error:
			glog.Errorf("Kafka error: %v", e)
		}

		if consumer.flushRequired() {
			err := consumer.flush(ctx)
			if err != nil {
				glog.Fatalf("Panic: Unable to flush samples to storage: %v", err)
			}

			tps, err := consumer.KafkaConsumer.Commit()
			if err != nil {
				glog.Fatalf("Panic: Unable to commit kafka offsets: %v", err)
			}
			for _, tp := range tps {
				glog.Infof("Kafka commit topic: %v partition: %v offset: %v", *tp.Topic, tp.Partition, tp.Offset)
			}
		}
	}
}
