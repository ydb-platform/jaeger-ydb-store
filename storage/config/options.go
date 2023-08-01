package config

import (
	"time"

	"github.com/ydb-platform/jaeger-ydb-store/schema"
)

type Options struct {
	BufferSize   int
	BatchSize    int
	BatchWorkers int

	IndexerBufferSize int
	IndexerMaxTraces  int
	IndexerMaxTTL     time.Duration

	DbAddress string
	DbPath    schema.DbPath

	PoolSize            int
	QueryCacheSize      int
	ConnectTimeout      time.Duration
	WriteTimeout        time.Duration
	RetryAttemptTimeout time.Duration
	WriteSvcOpCacheSize int // cache size for svc/operation index writer
	WriteMaxSpanAge     time.Duration

	ReadTimeout       time.Duration
	ReadQueryParallel int
	ReadOpLimit       uint64
	ReadSvcLimit      uint64
}
