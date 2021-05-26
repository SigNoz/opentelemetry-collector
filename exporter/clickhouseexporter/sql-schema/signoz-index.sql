CREATE TABLE signoz_index (
  timestamp DateTime CODEC(Delta, ZSTD(1)),
  traceID String CODEC(ZSTD(1)),
  service LowCardinality(String) CODEC(ZSTD(1)),
  name LowCardinality(String) CODEC(ZSTD(1)),
  durationNano UInt64 CODEC(ZSTD(1)),
  tags Array(String) CODEC(ZSTD(1)),
  tagKeys Array(String) CODEC(ZSTD(1)),
  tagValues Array(String) CODEC(ZSTD(1)),
  INDEX idx_tagKeys tagKeys TYPE bloom_filter(0.01) GRANULARITY 64,
  INDEX idx_tagValues tagValues TYPE bloom_filter(0.01) GRANULARITY 64,
  INDEX idx_duration durationNano TYPE minmax GRANULARITY 1
) ENGINE MergeTree()
PARTITION BY toDate(timestamp)
ORDER BY (service, -toUnixTimestamp(timestamp))
SETTINGS index_granularity=1024