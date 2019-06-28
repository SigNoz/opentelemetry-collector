// Copyright 2019, OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package nodebatcher

import (
	"time"

	"github.com/open-telemetry/opentelemetry-service/pkg/configmodels"
)

// ConfigV2 defines configuration for batch processor.
type ConfigV2 struct {
	configmodels.ProcessorSettings `mapstructure:",squash"`

	// Timeout sets the time after which a batch will be sent regardless of size.
	Timeout *time.Duration `mapstructure:"timeout,omitempty"`

	// SendBatchSize is the size of a batch which after hit, will trigger it to be sent.
	SendBatchSize *int `mapstructure:"send-batch-size,omitempty"`

	// NumTickers sets the number of tickers to use to divide the work of looping
	// over batch buckets. This is an advanced configuration option.
	NumTickers int `mapstructure:"num-tickers,omitempty"`

	// TickTime sets time interval at which the tickers tick. This is an advanced
	// configuration option.
	TickTime *time.Duration `mapstructure:"tick-time,omitempty"`

	// RemoveAfterTicks is the number of ticks that must pass without a span arriving
	// from a node after which the batcher for that node will be deleted. This is an
	// advanced configuration option.
	RemoveAfterTicks *int `mapstructure:"remove-after-ticks,omitempty"`
}
