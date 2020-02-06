package index

import (
	"github.com/YandexClassifieds/jaeger-ydb-store/storage/spanstore/dbmodel"
	"github.com/jaegertracing/jaeger/model"
	"github.com/yandex-cloud/ydb-go-sdk"
)

type serviceOperationIndex struct {
	baseIndex
	serviceName   string
	operationName string
}

func NewServiceOperationIndex(span *model.Span) Indexable {
	return serviceOperationIndex{
		baseIndex:     newBaseIndex(span),
		serviceName:   span.Process.ServiceName,
		operationName: span.OperationName,
	}
}

func (s serviceOperationIndex) Hash() uint64 {
	return dbmodel.HashData(s.serviceName, s.operationName)
}

func (s serviceOperationIndex) StructFields(bucket uint8) []ydb.StructValueOption {
	return []ydb.StructValueOption{
		ydb.StructFieldValue("idx_hash", ydb.Uint64Value(s.Hash())),
		ydb.StructFieldValue("rev_start_time", ydb.Int64Value(-s.startTime.UnixNano())),
	}
}
