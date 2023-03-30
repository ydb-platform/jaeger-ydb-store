package index

import (
	"github.com/jaegertracing/jaeger/model"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"

	"github.com/ydb-platform/jaeger-ydb-store/storage/spanstore/dbmodel"
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

func (s serviceOperationIndex) StructFields(bucket uint8) []types.StructValueOption {
	return []types.StructValueOption{
		types.StructFieldValue("idx_hash", types.Uint64Value(s.Hash())),
		types.StructFieldValue("rev_start_time", types.Int64Value(-s.startTime.UnixNano())),
	}
}
