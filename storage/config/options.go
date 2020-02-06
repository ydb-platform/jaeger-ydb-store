package config

import (
	"github.com/YandexClassifieds/jaeger-ydb-store/schema"
	"time"
)

type Options struct {
	BufferSize   int
	BatchSize    int
	BatchWorkers int

	IndexerBufferSize int
	IndexerMaxTraces  int
	IndexerMaxTTL     time.Duration

	DbAddress        string
	DbPath           schema.DbPath

	PoolSize       int
	QueryCacheSize int
	ConnectTimeout time.Duration
	WriteTimeout   time.Duration

	ReadTimeout time.Duration
}
