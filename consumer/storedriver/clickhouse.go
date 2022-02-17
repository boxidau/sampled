package storedriver

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/boxidau/sampled/consumer/config"
	"github.com/boxidau/sampled/consumer/sample"
	"github.com/golang/glog"
	"github.com/pkg/errors"
)

const TS_COLUMN string = "_sample_timestamp"

type ClickHouseColumnType string

const (
	Measure  ClickHouseColumnType = "Float64"
	Label    ClickHouseColumnType = "String"
	LabelSet ClickHouseColumnType = "Array(String)"
)

var fieldTypeToStorageType = map[sample.FieldType]ClickHouseColumnType{
	sample.Measure:  Measure,
	sample.Label:    Label,
	sample.LabelSet: LabelSet,
}

type ClickHouseColumn struct {
	Name string
	Type ClickHouseColumnType
}

type knownSchema map[string]map[string]bool

type ClickHouseDriver struct {
	database    string
	connection  driver.Conn
	schemaStore knownSchema
}

func NewClickHouseStoreDriver(cfg *config.ClickHouseConfig) (*ClickHouseDriver, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: cfg.Hosts,
		Auth: clickhouse.Auth{
			Database: cfg.Database,
			Username: cfg.Username,
			Password: cfg.Password,
		},
		//Debug:           true,
		DialTimeout:     time.Second * 3,
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: time.Hour,
	})

	if err != nil {
		return nil, errors.Wrap(err, "Unable to create ClickHouse connection")
	}

	hs, err := conn.ServerVersion()
	if err != nil {
		return nil, errors.Wrap(err, "Unable to establish ClickHouse connection")
	}

	glog.Infof("ClickHouse connection established: %s (%s) - %d.%d.%d database: %s",
		hs.DisplayName,
		hs.Name,
		hs.Version.Major, hs.Version.Minor, hs.Version.Patch,
		cfg.Database,
	)

	return &ClickHouseDriver{
		database:    cfg.Database,
		connection:  conn,
		schemaStore: knownSchema{},
	}, nil
}

func (chd ClickHouseDriver) createBaseDataset(ctx context.Context, dataset string) error {
	if _, ok := chd.schemaStore[dataset]; ok {
		glog.V(2).Infof("%s is a known dataset, skipping table creation", dataset)
		return nil
	}
	glog.V(2).Infof("Refreshing schema for %s", dataset)
	err := chd.connection.Exec(
		ctx,
		fmt.Sprintf(`
			CREATE TABLE IF NOT EXISTS %s.%s (%s DateTime) 
			ENGINE = MergeTree()
			PARTITION BY toYYYYMM(%s)
			ORDER BY %s;
			`,
			chd.database, dataset, TS_COLUMN, TS_COLUMN, TS_COLUMN,
		),
	)
	if err != nil {
		glog.Error(err)
		return err
	}
	chd.schemaStore[dataset] = map[string]bool{TS_COLUMN: true}
	return nil
}

func (chd ClickHouseDriver) createColumn(ctx context.Context, dataset, name string, columnType ClickHouseColumnType) error {
	knownColumns, ok := chd.schemaStore[dataset]
	if !ok {
		return fmt.Errorf("attempting to create column %s before dataset %s is known to be created", name, dataset)
	} else {
		if _, ok := knownColumns[name]; ok {
			glog.V(3).Infof("%s is a known column for %s, skipping creation attempt", name, dataset)
			return nil
		}
	}

	query := fmt.Sprintf("ALTER TABLE %s.%s ADD COLUMN IF NOT EXISTS %s %s", chd.database, dataset, name, columnType)
	glog.V(3).Infof("Create column SQL: %s", query)
	err := chd.connection.Exec(ctx, query)
	if err != nil {
		return err
	}
	knownColumns[name] = true
	return nil
}

// Takes a slice of samples and inserts them into the store
func (chd ClickHouseDriver) InsertSamples(ctx context.Context, samples []sample.Sample) error {

	samplesByDataset := map[string][]sample.Sample{}
	for _, s := range samples {
		if _, ok := samplesByDataset[s.Dataset]; !ok {
			samplesByDataset[s.Dataset] = []sample.Sample{s}
		} else {
			samplesByDataset[s.Dataset] = append(samplesByDataset[s.Dataset], s)
		}
	}
	for d, s := range samplesByDataset {
		err := chd.insertSamplesToDataset(ctx, d, s)
		if err != nil {
			return err
		}
	}

	return nil
}

func (chd ClickHouseDriver) insertSamplesToDataset(ctx context.Context, dataset string, samples []sample.Sample) error {
	fieldSet := map[string]ClickHouseColumn{}
	insertFieldOrder := []string{}

	err := chd.createBaseDataset(ctx, dataset)
	if err != nil {
		return err
	}

	for _, s := range samples {
		for _, f := range s.Data {
			if f.Name == TS_COLUMN {
				continue
			}
			if _, ok := fieldSet[f.Name]; !ok {
				colType, ok := fieldTypeToStorageType[f.Type]
				if !ok {
					return fmt.Errorf("unknown FieldType for clickhouse storage driver")
				}
				err := chd.createColumn(ctx, dataset, f.Name, colType)
				if err != nil {
					return err
				}
				fieldSet[f.Name] = ClickHouseColumn{Name: f.Name, Type: colType}
				insertFieldOrder = append(insertFieldOrder, f.Name)
			}
		}
	}

	batchQuery := fmt.Sprintf(
		"INSERT INTO %s.%s (%s, %s)",
		chd.database,
		dataset,
		TS_COLUMN,
		strings.Join(insertFieldOrder, ", "),
	)
	glog.V(3).Info(batchQuery)
	batch, err := chd.connection.PrepareBatch(ctx, batchQuery)
	if err != nil {
		return err
	}

	for _, s := range samples {
		// insert timestamp first
		batch.Column(0).Append([]time.Time{time.Unix(s.Timestamp/1000, 0).UTC()})
		for colIdx, fieldName := range insertFieldOrder {
			fieldDescription, ok := fieldSet[fieldName]
			if !ok {
				return fmt.Errorf("internally generated field description is missing, is your CPU/memory okay?")
			}
			fv, sampleHasField := s.Data[fieldName]
			switch fieldDescription.Type {
			case Measure:
				if sampleHasField {
					batch.Column(colIdx + 1).Append([]float64{fv.MeasureValue})
				} else {
					batch.Column(colIdx + 1).Append([]float64{0})
				}
			case Label:
				if sampleHasField {
					batch.Column(colIdx + 1).Append([]string{fv.LabelValue})
				} else {
					batch.Column(colIdx + 1).Append([]string{""})
				}
			case LabelSet:
				if sampleHasField {
					batch.Column(colIdx + 1).Append([][]string{fv.LabelSetValue})
				} else {
					batch.Column(colIdx + 1).Append([][]string{{}})
				}
			}
		}
	}
	err = batch.Send()
	if err != nil {
		return err
	}

	return nil
}

func (chd ClickHouseDriver) Close() {
	chd.connection.Close()
}

var _ StoreDriver = ClickHouseDriver{}
