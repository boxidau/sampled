package main

import (
	"context"
	"encoding/json"
	"flag"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/boxidau/sampled/consumer/config"
	"github.com/boxidau/sampled/consumer/sample"
	"github.com/boxidau/sampled/consumer/storedriver"
	"github.com/golang/glog"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

var FLAG_sampledConfig = flag.String("sampled_config", "config.yaml", "Sampled config file")

func init() {
	flag.Parse()
}

type Consumer struct {
	Config        *config.SampledConfig
	StoreDriver   storedriver.StoreDriver
	KafkaConsumer *kafka.Consumer
	lastFlush     time.Time
	sampleBuffer  []sample.Sample
	run           bool
}

func (c *Consumer) flushRequired() bool {
	if len(c.sampleBuffer) >= int(c.Config.Tuning.SampleBufferSize) {
		return true
	}

	if len(c.sampleBuffer) > 0 && time.Since(c.lastFlush) > c.Config.Tuning.FlushInterval {
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
	c.sampleBuffer = make([]sample.Sample, 0, c.Config.Tuning.SampleBufferSize)
	c.lastFlush = time.Now()
	return nil
}

func (c *Consumer) Close() {
	glog.Infof("Closing kafka consumer...")
	c.KafkaConsumer.Close()
	glog.Infof("Closing store driver...")
	c.StoreDriver.Close()
}

func (c *Consumer) Shutdown() {
	c.run = false
}

func (c *Consumer) Run() {
	glog.Infof("Subscribing to kafka topics: %v", c.Config.Kafka.Topics)
	c.KafkaConsumer.SubscribeTopics(c.Config.Kafka.Topics, nil)

	ctx := context.Background()
	for c.run {
		ev := c.KafkaConsumer.Poll(10)
		switch e := ev.(type) {
		case *kafka.Message:
			var s sample.RawSample
			err := json.Unmarshal(e.Value, &s)
			if err != nil {
				glog.Errorf("Unable to unmarshal sample json")
			} else {
				smp, err := sample.SampleFromRawSample(&s)
				if err != nil {
					glog.Errorf("Skipping invalid sample: %v", err)
				}
				c.sampleBuffer = append(c.sampleBuffer, smp)
			}
		case kafka.Error:
			if !e.IsRetriable() {
				glog.Fatalf("Fatal kafka error: %v", e)
			} else {
				glog.Errorf("Retryable kafka error: %v", e)
			}
		}

		if c.flushRequired() {
			err := c.flush(ctx)
			if err != nil {
				glog.Fatalf("Panic: Unable to flush samples to storage: %v", err)
			}

			tps, err := c.KafkaConsumer.Commit()
			if err != nil {
				glog.Fatalf("Panic: Unable to commit kafka offsets: %v", err)
			}
			for _, tp := range tps {
				glog.Infof("Kafka commit topic: %v partition: %v offset: %v", *tp.Topic, tp.Partition, tp.Offset)
			}
		}
	}
}

func NewConsumer(cfg *config.SampledConfig) (*Consumer, error) {
	consumer := &Consumer{
		Config:       cfg,
		run:          true,
		sampleBuffer: make([]sample.Sample, 0, cfg.Tuning.SampleBufferSize),
	}

	chd, err := storedriver.NewClickHouseStoreDriver(&cfg.Store.ClickHouse)
	if err != nil {
		return nil, err
	}
	consumer.StoreDriver = chd

	kafkaConsumer, err := NewKafkaConsumer(&cfg.Kafka)
	if err != nil {
		glog.Fatalf("Failed to setup kafka consumer: %v", err)
	}
	consumer.KafkaConsumer = kafkaConsumer

	return consumer, nil
}

func main() {
	cfg, err := config.LoadConfig(*FLAG_sampledConfig)
	if err != nil {
		glog.Fatal(err)
	}

	consumer, err := NewConsumer(cfg)
	if err != nil {
		glog.Fatal(err)
	}
	defer consumer.Close()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		glog.Info("Shutting down")
		consumer.Shutdown()
	}()

	glog.Info("Starting sampled consumer")
	consumer.Run()
}
