package indexer

import (
	"time"

	"github.com/yandex-cloud/jaeger-ydb-store/schema"
	"github.com/yandex-cloud/jaeger-ydb-store/storage/spanstore/batch"
)

type Options struct {
	DbPath       schema.DbPath
	MaxTraces    int
	MaxTTL       time.Duration
	BufferSize   int
	Batch        batch.Options
	WriteTimeout time.Duration
}
