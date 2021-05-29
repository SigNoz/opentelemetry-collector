CREATE TABLE signoz_index (
  timestamp DateTime64(9) CODEC(Delta, ZSTD(1)),
  traceID String CODEC(ZSTD(1)),
  spanID String CODEC(ZSTD(1)),
  serviceName LowCardinality(String) CODEC(ZSTD(1)),
  name LowCardinality(String) CODEC(ZSTD(1)),
  kind Int32 CODEC(ZSTD(1)),
  durationNano UInt64 CODEC(ZSTD(1)),
  tags Array(String) CODEC(ZSTD(1)),
  tagsKeys Array(String) CODEC(ZSTD(1)),
  tagsValues Array(String) CODEC(ZSTD(1)),
  statusCode Int64 CODEC(ZSTD(1)),
  externalHttpMethod String CODEC(ZSTD(1)),
  externalHttpUrl String CODEC(ZSTD(1)),
  component String CODEC(ZSTD(1)),
  dBSystem String CODEC(ZSTD(1)),
  dBName String CODEC(ZSTD(1)),
  dBOperation String CODEC(ZSTD(1)),
  peerService String CODEC(ZSTD(1)),
  INDEX idx_tagsKeys tagsKeys TYPE bloom_filter(0.01) GRANULARITY 64,
  INDEX idx_tagsValues tagsValues TYPE bloom_filter(0.01) GRANULARITY 64,
  INDEX idx_duration durationNano TYPE minmax GRANULARITY 1
) ENGINE MergeTree()
PARTITION BY toDate(timestamp)
ORDER BY (serviceName, -toUnixTimestamp(timestamp))
SETTINGS index_granularity=1024