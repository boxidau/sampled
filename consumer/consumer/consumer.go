package consumer

import (
	"context"
	"encoding/json"
	"time"

	"github.com/boxidau/sampled/consumer/config"
	"github.com/boxidau/sampled/consumer/sample"
	"github.com/boxidau/sampled/consumer/storedriver"
	"github.com/golang/glog"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

var (
	samplesValidCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "sampled_valid_samples_total",
		Help: "The total number of processed samples",
	})
	samplesInvalidCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "sampled_invalid_samples_total",
		Help: "The total number of samples which failed to parse",
	})
	batchSizeGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "sampled_batch_size",
		Help: "Size of batch during flush to storage",
	})
	flushSuccess = promauto.NewCounter(prometheus.CounterOpts{
		Name: "sampled_flush_success_total",
		Help: "The total nmber of successful sample flush events",
	})
	kafkaRetriableErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "sampled_kafka_retriable_errors_total",
		Help: "The total number of kafka errors encountered",
	})
	flushTimerHistogram = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "sampled_storage_flush_duration_seconds",
		Help:    "Time taken to flush samples to storage",
		Buckets: prometheus.ExponentialBucketsRange(0.1, 10, 20),
	})
	kafkaComitTimerHistogram = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "sampled_kafka_commit_duration_seconds",
		Help:    "Time taken to commit offsets to kafka",
		Buckets: prometheus.ExponentialBucketsRange(0.1, 10, 20),
	})
)

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
	batchSizeGauge.Set(float64(curBatchSize))
	if curBatchSize == 0 {
		return nil
	}

	glog.Infof("Flushing batch of %d samples to storage", curBatchSize)
	fTimer := prometheus.NewTimer(flushTimerHistogram)
	err := c.StoreDriver.InsertSamples(ctx, c.sampleBuffer)
	if err != nil {
		return err
	}
	fTimer.ObserveDuration()
	c.sampleBuffer = make([]sample.Sample, 0, c.Config.Tuning.SampleBufferSize)

	kTimer := prometheus.NewTimer(kafkaComitTimerHistogram)
	tps, err := c.KafkaConsumer.Commit()
	kTimer.ObserveDuration()
	if err != nil {
		return errors.Wrap(err, "Unable to commit kafka offsets: %v")
	}
	for _, tp := range tps {
		glog.Infof("Kafka commit topic: %v partition: %v offset: %v", *tp.Topic, tp.Partition, tp.Offset)
	}

	c.lastFlush = time.Now()
	return nil
}

func (c *Consumer) Shutdown() {
	c.run = false
}

func (c *Consumer) Run() error {
	glog.Infof("Subscribing to kafka topics: %v", c.Config.Kafka.Topics)
	err := c.KafkaConsumer.SubscribeTopics(c.Config.Kafka.Topics, nil)
	if err != nil {
		return err
	}

	ctx := context.Background()
	for c.run {
		ev := c.KafkaConsumer.Poll(10)
		switch e := ev.(type) {
		case *kafka.Message:
			var s sample.RawSample
			err := json.Unmarshal(e.Value, &s)
			if err != nil {
				samplesInvalidCounter.Inc()
				glog.Errorf("Unable to unmarshal sample json")
			} else {
				smp, err := sample.SampleFromRawSample(&s)
				if err != nil {
					samplesInvalidCounter.Inc()
					glog.Errorf("Skipping invalid sample: %v", err)
				} else {
					samplesValidCounter.Inc()
					c.sampleBuffer = append(c.sampleBuffer, smp)
				}
			}
		case kafka.Error:
			if !e.IsRetriable() {
				glog.Fatalf("Fatal kafka error: %v", e)
			} else {
				kafkaRetriableErrors.Inc()
				glog.Errorf("Retryable kafka error: %v", e)
			}
		}

		if c.flushRequired() {
			err := c.flush(ctx)
			if err != nil {
				glog.Fatalf("Panic: Unable to flush samples to storage: %v", err)
			}
			flushSuccess.Inc()
		}
	}

	glog.Infof("Closing kafka consumer...")
	err = c.KafkaConsumer.Close()
	if err != nil {
		return err
	}
	glog.Infof("Closing store driver...")
	err = c.StoreDriver.Close()
	if err != nil {
		return err
	}
	return nil
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
