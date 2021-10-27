package index

import (
	"github.com/jaegertracing/jaeger/model"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"

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

func (t tagIndex) StructFields(bucket uint8) []types.StructValueOption {
	return []types.StructValueOption{
		types.StructFieldValue("idx_hash", types.Uint64Value(dbmodel.HashTagIndex(t.serviceName, t.key, t.value, bucket))),
		types.StructFieldValue("rev_start_time", types.Int64Value(-t.startTime.UnixNano())),
		types.StructFieldValue("op_hash", types.Uint64Value(dbmodel.HashData(t.opName))),
	}
}
