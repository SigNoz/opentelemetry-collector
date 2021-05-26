package clickhouseexporter

import (
<<<<<<< HEAD
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"

	"go.uber.org/zap"
=======
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"go.uber.org/zap"

	"github.com/jaegertracing/jaeger/model"
>>>>>>> 9f5c01d9fcd5836b0745240db33f8a8d0ee16087
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
<<<<<<< HEAD
	db         *sqlx.DB
=======
	db         *sql.DB
>>>>>>> 9f5c01d9fcd5836b0745240db33f8a8d0ee16087
	indexTable string
	spansTable string
	encoding   Encoding
	delay      time.Duration
	size       int
<<<<<<< HEAD
	spans      chan *Span
=======
	spans      chan *model.Span
>>>>>>> 9f5c01d9fcd5836b0745240db33f8a8d0ee16087
	finish     chan bool
	done       sync.WaitGroup
}

// NewSpanWriter returns a SpanWriter for the database
<<<<<<< HEAD
func NewSpanWriter(logger *zap.Logger, db *sqlx.DB, indexTable string, spansTable string, encoding Encoding, delay time.Duration, size int) *SpanWriter {
=======
func NewSpanWriter(logger *zap.Logger, db *sql.DB, indexTable string, spansTable string, encoding Encoding, delay time.Duration, size int) *SpanWriter {
>>>>>>> 9f5c01d9fcd5836b0745240db33f8a8d0ee16087
	writer := &SpanWriter{
		logger:     logger,
		db:         db,
		indexTable: indexTable,
		spansTable: spansTable,
		encoding:   encoding,
		delay:      delay,
		size:       size,
<<<<<<< HEAD
		spans:      make(chan *Span, size),
=======
		spans:      make(chan *model.Span, size),
>>>>>>> 9f5c01d9fcd5836b0745240db33f8a8d0ee16087
		finish:     make(chan bool),
	}

	go writer.backgroundWriter()

	return writer
}

func (w *SpanWriter) backgroundWriter() {
<<<<<<< HEAD
	batch := make([]*Span, 0, w.size)
=======
	batch := make([]*model.Span, 0, w.size)
>>>>>>> 9f5c01d9fcd5836b0745240db33f8a8d0ee16087

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

<<<<<<< HEAD
			batch = make([]*Span, 0, w.size)
=======
			batch = make([]*model.Span, 0, w.size)
>>>>>>> 9f5c01d9fcd5836b0745240db33f8a8d0ee16087
			last = time.Now()
		}

		w.done.Done()

		if finish {
			break
		}
	}
}

<<<<<<< HEAD
func (w *SpanWriter) writeBatch(batch []*Span) error {
=======
func (w *SpanWriter) writeBatch(batch []*model.Span) error {
>>>>>>> 9f5c01d9fcd5836b0745240db33f8a8d0ee16087
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

<<<<<<< HEAD
func (w *SpanWriter) writeModelBatch(batch []*Span) error {
=======
func (w *SpanWriter) writeModelBatch(batch []*model.Span) error {
>>>>>>> 9f5c01d9fcd5836b0745240db33f8a8d0ee16087
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
<<<<<<< HEAD
			// serialized, err = proto.Marshal(span)
		}
=======
			serialized, err = proto.Marshal(span)
		}

>>>>>>> 9f5c01d9fcd5836b0745240db33f8a8d0ee16087
		if err != nil {
			return err
		}

<<<<<<< HEAD
		_, err = statement.Exec(span.StartTimeUnixNano, span.TraceId, serialized)
=======
		_, err = statement.Exec(span.StartTime, span.TraceID.String(), serialized)
>>>>>>> 9f5c01d9fcd5836b0745240db33f8a8d0ee16087
		if err != nil {
			return err
		}
	}

	commited = true

	return tx.Commit()
}

<<<<<<< HEAD
func (w *SpanWriter) writeIndexBatch(batch []*Span) error {
=======
func (w *SpanWriter) writeIndexBatch(batch []*model.Span) error {
>>>>>>> 9f5c01d9fcd5836b0745240db33f8a8d0ee16087
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

<<<<<<< HEAD
	statement, err := tx.Prepare(fmt.Sprintf("INSERT INTO %s (timestamp, traceID, service, name, durationNano, tags, tagKeys, tagValues) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", w.indexTable))
=======
	statement, err := tx.Prepare(fmt.Sprintf("INSERT INTO %s (timestamp, traceID, service, operation, durationUs, tags) VALUES (?, ?, ?, ?, ?, ?)", w.indexTable))
>>>>>>> 9f5c01d9fcd5836b0745240db33f8a8d0ee16087
	if err != nil {
		return err
	}

	defer statement.Close()

	for _, span := range batch {
		_, err = statement.Exec(
<<<<<<< HEAD
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
=======
			span.StartTime,
			span.TraceID.String(),
			span.Process.ServiceName,
			span.OperationName,
			span.Duration.Microseconds(),
			uniqueTagsForSpan(span),
>>>>>>> 9f5c01d9fcd5836b0745240db33f8a8d0ee16087
		)
		if err != nil {
			return err
		}
	}

	commited = true

	return tx.Commit()
}

// WriteSpan writes the encoded span
<<<<<<< HEAD
func (w *SpanWriter) WriteSpan(span *Span) error {
=======
func (w *SpanWriter) WriteSpan(span *model.Span) error {
>>>>>>> 9f5c01d9fcd5836b0745240db33f8a8d0ee16087
	w.spans <- span
	return nil
}

// Close Implements io.Closer and closes the underlying storage
func (w *SpanWriter) Close() error {
	w.finish <- true
	w.done.Wait()
	return nil
}
<<<<<<< HEAD
=======

func uniqueTagsForSpan(span *model.Span) []string {
	uniqueTags := make(map[string]struct{}, len(span.Tags)+len(span.Process.Tags))

	buf := &strings.Builder{}

	for _, kv := range span.Tags {
		uniqueTags[tagString(buf, &kv)] = struct{}{}
	}

	for _, kv := range span.Process.Tags {
		uniqueTags[tagString(buf, &kv)] = struct{}{}
	}

	for _, event := range span.Logs {
		for _, kv := range event.Fields {
			uniqueTags[tagString(buf, &kv)] = struct{}{}
		}
	}

	tags := make([]string, 0, len(uniqueTags))

	for kv := range uniqueTags {
		tags = append(tags, kv)
	}

	sort.Strings(tags)

	return tags
}

func tagString(buf *strings.Builder, kv *model.KeyValue) string {
	buf.Reset()

	buf.WriteString(kv.Key)
	buf.WriteByte('=')
	buf.WriteString(kv.AsString())

	return buf.String()
}
>>>>>>> 9f5c01d9fcd5836b0745240db33f8a8d0ee16087
