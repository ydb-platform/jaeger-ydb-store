package writer

import (
	"time"

	"github.com/ydb-platform/jaeger-ydb-store/schema"
)

type BatchWriterOptions struct {
	DbPath              schema.DbPath
	WriteTimeout        time.Duration
	RetryAttemptTimeout time.Duration
}

type SpanWriterOptions struct {
	BufferSize          int
	BatchSize           int
	BatchWorkers        int
	IndexerBufferSize   int
	IndexerMaxTraces    int
	IndexerTTL          time.Duration
	DbPath              schema.DbPath
	WriteTimeout        time.Duration
	RetryAttemptTimeout time.Duration
	ArchiveWriter       bool
	OpCacheSize         int
	MaxSpanAge          time.Duration
}
