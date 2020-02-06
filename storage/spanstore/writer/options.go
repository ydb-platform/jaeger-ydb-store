package writer

import (
	"github.com/YandexClassifieds/jaeger-ydb-store/schema"
	"time"
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
}
