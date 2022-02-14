package storedriver

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
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

type ClickHouseDriver struct {
	database   string
	connection driver.Conn
}

func NewClickHouseStoreDriver(uri string) (*ClickHouseDriver, error) {

	dsn, err := url.Parse(uri)
	if err != nil {
		glog.Fatalf("Invalid clickhouse_dsn %s: %v", uri, err)
	}

	password, ok := dsn.User.Password()
	if !ok {
		password = ""
	}

	hosts := strings.Split(dsn.Host, ",")
	database := strings.TrimPrefix(dsn.Path, "/")

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: hosts,
		Auth: clickhouse.Auth{
			Database: database,
			Username: dsn.User.Username(),
			Password: password,
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
		database,
	)

	return &ClickHouseDriver{database: database, connection: conn}, nil
}

func (chd ClickHouseDriver) CreateBaseDataset(ctx context.Context, dataset string) error {
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
		err := chd.InsertSamplesToDataset(ctx, d, s)
		if err != nil {
			return err
		}
	}

	return nil
}

func (chd ClickHouseDriver) InsertSamplesToDataset(ctx context.Context, dataset string, samples []sample.Sample) error {
	fieldSet := map[string]ClickHouseColumn{}
	insertFieldOrder := []string{}

	err := chd.CreateBaseDataset(ctx, dataset)
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
				err := chd.CreateColumn(ctx, dataset, f.Name, colType)
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
					batch.Column(colIdx + 1).Append([][]string{})
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

func (chd ClickHouseDriver) CreateColumn(ctx context.Context, dataset, name string, columnType ClickHouseColumnType) error {
	query := fmt.Sprintf("ALTER TABLE %s.%s ADD COLUMN IF NOT EXISTS %s %s", chd.database, dataset, name, columnType)
	glog.V(3).Infof("Create column SQL: %s", query)
	err := chd.connection.Exec(ctx, query)
	if err != nil {
		return err
	}
	return nil
}

func (chd ClickHouseDriver) Close() {
	chd.connection.Close()
}

var _ StoreDriver = ClickHouseDriver{}
