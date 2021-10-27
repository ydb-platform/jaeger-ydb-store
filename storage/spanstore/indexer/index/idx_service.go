package index

import (
	"github.com/jaegertracing/jaeger/model"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"

	"github.com/yandex-cloud/jaeger-ydb-store/storage/spanstore/dbmodel"
)

type serviceNameIndex struct {
	baseIndex
	serviceName string
}

func NewServiceNameIndex(span *model.Span) Indexable {
	return serviceNameIndex{
		baseIndex:   newBaseIndex(span),
		serviceName: span.Process.ServiceName,
	}
}

func (s serviceNameIndex) Hash() uint64 {
	return dbmodel.HashData(s.serviceName)
}

func (s serviceNameIndex) StructFields(bucket uint8) []types.StructValueOption {
	return []types.StructValueOption{
		types.StructFieldValue("idx_hash", types.Uint64Value(dbmodel.HashBucketData(bucket, s.serviceName))),
		types.StructFieldValue("rev_start_time", types.Int64Value(-s.startTime.UnixNano())),
	}
}
