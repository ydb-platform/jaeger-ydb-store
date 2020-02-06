package index

import (
	"github.com/YandexClassifieds/jaeger-ydb-store/storage/spanstore/dbmodel"
	"github.com/jaegertracing/jaeger/model"
	"github.com/yandex-cloud/ydb-go-sdk"
)

type tagIndex struct {
	baseIndex
	serviceName string
	key         string
	value       string
}

func NewTagIndex(span *model.Span, kv model.KeyValue) Indexable {
	return tagIndex{
		baseIndex:   newBaseIndex(span),
		serviceName: span.Process.ServiceName,
		key:         kv.Key,
		value:       kv.AsString(),
	}
}

func (t tagIndex) Hash() uint64 {
	return dbmodel.HashData(t.serviceName, t.key, t.value)
}

func (t tagIndex) StructFields(bucket uint8) []ydb.StructValueOption {
	return []ydb.StructValueOption{
		ydb.StructFieldValue("idx_hash", ydb.Uint64Value(dbmodel.HashTagIndex(t.serviceName, t.key, t.value, bucket))),
		ydb.StructFieldValue("rev_start_time", ydb.Int64Value(-t.startTime.UnixNano())),
	}
}
