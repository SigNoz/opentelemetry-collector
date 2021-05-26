package clickhouseexporter

import (
<<<<<<< HEAD
=======
	"database/sql"
>>>>>>> 9f5c01d9fcd5836b0745240db33f8a8d0ee16087
	"flag"
	"fmt"
	"time"

<<<<<<< HEAD
	"github.com/jmoiron/sqlx"

	_ "github.com/ClickHouse/clickhouse-go"

=======
	_ "github.com/ClickHouse/clickhouse-go"
	"github.com/jaegertracing/jaeger/model"
>>>>>>> 9f5c01d9fcd5836b0745240db33f8a8d0ee16087
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

// Factory implements storage.Factory for Clickhouse backend.
type Factory struct {
	logger     *zap.Logger
	Options    *Options
<<<<<<< HEAD
	db         *sqlx.DB
	archive    *sqlx.DB
=======
	db         *sql.DB
	archive    *sql.DB
>>>>>>> 9f5c01d9fcd5836b0745240db33f8a8d0ee16087
	datasource string
	makeWriter writerMaker
}

// Writer writes spans to storage.
type Writer interface {
<<<<<<< HEAD
	WriteSpan(span *Span) error
}

type writerMaker func(logger *zap.Logger, db *sqlx.DB, indexTable string, spansTable string, encoding Encoding, delay time.Duration, size int) (Writer, error)
=======
	WriteSpan(span *model.Span) error
}

type writerMaker func(logger *zap.Logger, db *sql.DB, indexTable string, spansTable string, encoding Encoding, delay time.Duration, size int) (Writer, error)
>>>>>>> 9f5c01d9fcd5836b0745240db33f8a8d0ee16087

// NewFactory creates a new Factory.
func ClickHouseNewFactory(datasource string) *Factory {
	return &Factory{
		Options: NewOptions(datasource, primaryNamespace, archiveNamespace),
		// makeReader: func(db *sql.DB, operationsTable, indexTable, spansTable string) (spanstore.Reader, error) {
		// 	return store.NewTraceReader(db, operationsTable, indexTable, spansTable), nil
		// },
<<<<<<< HEAD
		makeWriter: func(logger *zap.Logger, db *sqlx.DB, indexTable string, spansTable string, encoding Encoding, delay time.Duration, size int) (Writer, error) {
=======
		makeWriter: func(logger *zap.Logger, db *sql.DB, indexTable string, spansTable string, encoding Encoding, delay time.Duration, size int) (Writer, error) {
>>>>>>> 9f5c01d9fcd5836b0745240db33f8a8d0ee16087
			return NewSpanWriter(logger, db, indexTable, spansTable, encoding, delay, size), nil
		},
	}
}

// Initialize implements storage.Factory
func (f *Factory) Initialize(logger *zap.Logger) error {
	f.logger = logger

	db, err := f.connect(f.Options.getPrimary())
	if err != nil {
		return fmt.Errorf("error connecting to primary db: %v", err)
	}

	f.db = db

	archiveConfig := f.Options.others[archiveNamespace]
	if archiveConfig.Enabled {
		archive, err := f.connect(archiveConfig)
		if err != nil {
			return fmt.Errorf("error connecting to archive db: %v", err)
		}

		f.archive = archive
	}

	return nil
}

<<<<<<< HEAD
func (f *Factory) connect(cfg *namespaceConfig) (*sqlx.DB, error) {
=======
func (f *Factory) connect(cfg *namespaceConfig) (*sql.DB, error) {
>>>>>>> 9f5c01d9fcd5836b0745240db33f8a8d0ee16087
	if cfg.Encoding != EncodingJSON && cfg.Encoding != EncodingProto {
		return nil, fmt.Errorf("unknown encoding %q, supported: %q, %q", cfg.Encoding, EncodingJSON, EncodingProto)
	}

	return cfg.Connector(cfg)
}

// AddFlags implements plugin.Configurable
func (f *Factory) AddFlags(flagSet *flag.FlagSet) {
	f.Options.AddFlags(flagSet)
}

// InitFromViper implements plugin.Configurable
func (f *Factory) InitFromViper(v *viper.Viper) {
	f.Options.InitFromViper(v)
}

// CreateSpanWriter implements storage.Factory
func (f *Factory) CreateSpanWriter() (Writer, error) {
	cfg := f.Options.getPrimary()
	return f.makeWriter(f.logger, f.db, cfg.IndexTable, cfg.SpansTable, cfg.Encoding, cfg.WriteBatchDelay, cfg.WriteBatchSize)
}

// CreateArchiveSpanWriter implements storage.ArchiveFactory
func (f *Factory) CreateArchiveSpanWriter() (Writer, error) {
	if f.archive == nil {
		return nil, nil
	}
	cfg := f.Options.others[archiveNamespace]
	return f.makeWriter(f.logger, f.archive, "", cfg.SpansTable, cfg.Encoding, cfg.WriteBatchDelay, cfg.WriteBatchSize)
}

// Close Implements io.Closer and closes the underlying storage
func (f *Factory) Close() error {
	if f.db != nil {
		err := f.db.Close()
		if err != nil {
			return err
		}

		f.db = nil
	}

	if f.archive != nil {
		err := f.archive.Close()
		if err != nil {
			return err
		}

		f.archive = nil
	}

	return nil
}
