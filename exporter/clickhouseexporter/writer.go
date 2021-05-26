package clickhouseexporter

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"

	"go.uber.org/zap"
)

type Encoding string

const (
	// EncodingJSON is used for spans encoded as JSON.
	EncodingJSON Encoding = "json"
	// EncodingProto is used for spans encoded as Protobuf.
	EncodingProto Encoding = "protobuf"
)

// SpanWriter for writing spans to ClickHouse
type SpanWriter struct {
	logger     *zap.Logger
	db         *sqlx.DB
	indexTable string
	spansTable string
	encoding   Encoding
	delay      time.Duration
	size       int
	spans      chan *Span
	finish     chan bool
	done       sync.WaitGroup
}

// NewSpanWriter returns a SpanWriter for the database
func NewSpanWriter(logger *zap.Logger, db *sqlx.DB, indexTable string, spansTable string, encoding Encoding, delay time.Duration, size int) *SpanWriter {
	writer := &SpanWriter{
		logger:     logger,
		db:         db,
		indexTable: indexTable,
		spansTable: spansTable,
		encoding:   encoding,
		delay:      delay,
		size:       size,
		spans:      make(chan *Span, size),
		finish:     make(chan bool),
	}

	go writer.backgroundWriter()

	return writer
}

func (w *SpanWriter) backgroundWriter() {
	batch := make([]*Span, 0, w.size)

	timer := time.After(w.delay)
	last := time.Now()

	for {
		w.done.Add(1)

		flush := false
		finish := false

		select {
		case span := <-w.spans:
			batch = append(batch, span)
			flush = len(batch) == cap(batch)
		case <-timer:
			timer = time.After(w.delay)
			flush = time.Since(last) > w.delay && len(batch) > 0
		case <-w.finish:
			finish = true
			flush = len(batch) > 0
		}

		if flush {
			if err := w.writeBatch(batch); err != nil {
				w.logger.Error("Could not write a batch of spans", zap.Error(err))
			}

			batch = make([]*Span, 0, w.size)
			last = time.Now()
		}

		w.done.Done()

		if finish {
			break
		}
	}
}

func (w *SpanWriter) writeBatch(batch []*Span) error {
	if err := w.writeModelBatch(batch); err != nil {
		return err
	}

	if w.indexTable != "" {
		if err := w.writeIndexBatch(batch); err != nil {
			return err
		}
	}

	return nil
}

func (w *SpanWriter) writeModelBatch(batch []*Span) error {
	tx, err := w.db.Begin()
	if err != nil {
		return err
	}

	commited := false

	defer func() {
		if !commited {
			// Clickhouse does not support real rollback
			_ = tx.Rollback()
		}
	}()

	statement, err := tx.Prepare(fmt.Sprintf("INSERT INTO %s (timestamp, traceID, model) VALUES (?, ?, ?)", w.spansTable))
	if err != nil {
		return nil
	}

	defer statement.Close()

	for _, span := range batch {
		var serialized []byte

		if w.encoding == EncodingJSON {
			serialized, err = json.Marshal(span)
		} else {
			// serialized, err = proto.Marshal(span)
		}
		if err != nil {
			return err
		}

		_, err = statement.Exec(span.StartTimeUnixNano, span.TraceId, serialized)
		if err != nil {
			return err
		}
	}

	commited = true

	return tx.Commit()
}

func (w *SpanWriter) writeIndexBatch(batch []*Span) error {
	tx, err := w.db.Begin()
	if err != nil {
		return err
	}

	commited := false

	defer func() {
		if !commited {
			// Clickhouse does not support real rollback
			_ = tx.Rollback()
		}
	}()

	statement, err := tx.Prepare(fmt.Sprintf("INSERT INTO %s (timestamp, traceID, service, name, durationNano, tags, tagKeys, tagValues) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", w.indexTable))
	if err != nil {
		return err
	}

	defer statement.Close()

	for _, span := range batch {
		_, err = statement.Exec(
			span.StartTimeUnixNano,
			span.TraceId,
			span.ServiceName,
			span.Name,
			span.DurationNano,
			span.Tags,
			span.TagsKeys,
			span.TagsValues,
			// clickhouse.Array(span.Tags),
			// clickhouse.Array(span.TagsKeys),
			// clickhouse.Array(span.TagsValues),
		)
		if err != nil {
			return err
		}
	}

	commited = true

	return tx.Commit()
}

// WriteSpan writes the encoded span
func (w *SpanWriter) WriteSpan(span *Span) error {
	w.spans <- span
	return nil
}

// Close Implements io.Closer and closes the underlying storage
func (w *SpanWriter) Close() error {
	w.finish <- true
	w.done.Wait()
	return nil
}
