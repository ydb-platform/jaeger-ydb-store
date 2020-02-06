package index

import (
	"github.com/YandexClassifieds/jaeger-ydb-store/storage/spanstore/dbmodel"
	"github.com/jaegertracing/jaeger/model"
	"github.com/yandex-cloud/ydb-go-sdk"
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

func (s serviceNameIndex) StructFields(bucket uint8) []ydb.StructValueOption {
	return []ydb.StructValueOption{
		ydb.StructFieldValue("idx_hash", ydb.Uint64Value(dbmodel.HashBucketData(bucket, s.serviceName))),
		ydb.StructFieldValue("rev_start_time", ydb.Int64Value(-s.startTime.UnixNano())),
	}
}
