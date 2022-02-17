package storedriver

import (
	"context"

	"github.com/boxidau/sampled/consumer/sample"
)

type StoreDriver interface {
	// Takes a slice of samples and inserts them into the store
	InsertSamples(ctx context.Context, samples []sample.Sample) error

	// Close down any open connections/handles
	Close() error
}
