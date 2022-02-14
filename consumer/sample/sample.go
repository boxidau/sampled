package sample

import (
	"fmt"
	"regexp"

	"github.com/golang/glog"
)

type RawSample struct {
	Dataset   string                 `json:"dataset"`
	Timestamp int64                  `json:"timestamp"`
	Sample    map[string]interface{} `json:"sample"`
}

type FieldType int

const (
	Measure FieldType = iota
	Label
	LabelSet
)

type Field struct {
	Type          FieldType
	Name          string
	MeasureValue  float64
	LabelValue    string
	LabelSetValue []string
}

type Sample struct {
	Dataset   string
	Timestamp int64
	Data      map[string]Field
}

var ValidStr = regexp.MustCompile(`^[a-zA-Z0-9_]+$`).MatchString

func SampleFromRawSample(r *RawSample) (Sample, error) {
	sample := Sample{}
	if !ValidStr(r.Dataset) {
		return sample, fmt.Errorf("invalid dataset name '%s' specified in sample", r.Dataset)
	}
	sample.Dataset = r.Dataset
	sample.Timestamp = r.Timestamp
	sample.Data = map[string]Field{}

	for k, v := range r.Sample {
		if !ValidStr(k) {
			glog.Warningf("Skipping invalid field '%s' specified for dataset %s", k, sample.Dataset)
			continue
		}
		switch t := v.(type) {
		case string:
			sample.Data[k] = Field{Name: k, Type: Label, LabelValue: v.(string)}
		case float64:
			sample.Data[k] = Field{Name: k, Type: Measure, MeasureValue: v.(float64)}
		case int64:
			sample.Data[k] = Field{Name: k, Type: Measure, MeasureValue: v.(float64)}
		default:
			glog.Infof("Unsupported sample value type %s for %s: %s", t, k, v)
		}
	}
	return sample, nil
}
