package writer

import (
	"time"

	"github.com/yandex-cloud/jaeger-ydb-store/schema"
)

type BatchWriterOptions struct {
	DbPath       schema.DbPath
	WriteTimeout time.Duration
}

type SpanWriterOptions struct {
	BufferSize        int
	BatchSize         int
	BatchWorkers      int
	IndexerBufferSize int
	IndexerMaxTraces  int
	IndexerTTL        time.Duration
	DbPath            schema.DbPath
	WriteTimeout      time.Duration
	ArchiveWriter     bool
	CacheSize         int
}
