package index

import (
	"time"

	"github.com/jaegertracing/jaeger/model"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

type Indexable interface {
	Hash() uint64
	StructFields(bucket uint8) []types.StructValueOption
	Timestamp() time.Time
}

type baseIndex struct {
	startTime time.Time
}

func newBaseIndex(span *model.Span) baseIndex {
	return baseIndex{
		startTime: span.StartTime,
	}
}

func (i baseIndex) Timestamp() time.Time {
	return i.startTime
}
