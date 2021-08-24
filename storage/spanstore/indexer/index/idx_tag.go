package index

import (
	"github.com/jaegertracing/jaeger/model"
	"github.com/yandex-cloud/ydb-go-sdk/v2"

	"github.com/yandex-cloud/jaeger-ydb-store/storage/spanstore/dbmodel"
)

type tagIndex struct {
	baseIndex
	serviceName string
	opName      string
	key         string
	value       string
}

func NewTagIndex(span *model.Span, kv model.KeyValue) Indexable {
	return tagIndex{
		baseIndex:   newBaseIndex(span),
		serviceName: span.GetProcess().GetServiceName(),
		opName:      span.GetOperationName(),
		key:         kv.Key,
		value:       kv.AsString(),
	}
}

func (t tagIndex) Hash() uint64 {
	return dbmodel.HashData(t.serviceName, t.opName, t.key, t.value)
}

func (t tagIndex) StructFields(bucket uint8) []ydb.StructValueOption {
	return []ydb.StructValueOption{
		ydb.StructFieldValue("idx_hash", ydb.Uint64Value(dbmodel.HashTagIndex(t.serviceName, t.key, t.value, bucket))),
		ydb.StructFieldValue("rev_start_time", ydb.Int64Value(-t.startTime.UnixNano())),
		ydb.StructFieldValue("op_hash", ydb.Uint64Value(dbmodel.HashData(t.opName))),
	}
}
