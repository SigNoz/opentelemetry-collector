// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package clickhouseexporter

import (
	"context"

	"go.opentelemetry.io/collector/config"
	"go.opentelemetry.io/collector/consumer/pdata"
	jaegertranslator "go.opentelemetry.io/collector/translator/trace/jaeger"
	"go.uber.org/zap"
)

// Crete new exporter.
func newExporter(cfg config.Exporter, logger *zap.Logger) (*storage, error) {

	configClickHouse := cfg.(*Config)

	f := ClickHouseNewFactory(configClickHouse.Datasource)

	err := f.Initialize(logger)
	if err != nil {
		return nil, err
	}

	spanWriter, err := f.CreateSpanWriter()
	if err != nil {
		return nil, err
	}
	storage := storage{Writer: spanWriter}

	return &storage, nil
}

type storage struct {
	Writer Writer
}

// traceDataPusher implements OTEL exporterhelper.traceDataPusher
func (s *storage) pushTraceData(ctx context.Context, td pdata.Traces) error {

	// Need to improvise the error that is returned
	batches, err := jaegertranslator.InternalTracesToJaegerProto(td)
	if err != nil {
		return err
	}
	dropped := 0
	var errs []error
	for _, batch := range batches {
		for _, span := range batch.Spans {
			span.Process = batch.Process
			err := s.Writer.WriteSpan(span)
			if err != nil {
				errs = append(errs, err)
				dropped++
				zap.S().Error("Error in writing spans in clickhouse: ", err)
			}
		}
	}
	return nil
}
