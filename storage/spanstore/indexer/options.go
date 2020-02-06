package indexer

import (
	"github.com/YandexClassifieds/jaeger-ydb-store/schema"
	"github.com/YandexClassifieds/jaeger-ydb-store/storage/spanstore/batch"
	"time"
)

type Options struct {
	DbPath       schema.DbPath
	MaxTraces    int
	MaxTTL       time.Duration
	BufferSize   int
	Batch        batch.Options
	WriteTimeout time.Duration
}
