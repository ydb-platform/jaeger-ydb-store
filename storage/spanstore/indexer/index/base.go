package index

import (
	"time"

	"github.com/jaegertracing/jaeger/model"
	"github.com/yandex-cloud/ydb-go-sdk/v2"
)

type Indexable interface {
	Hash() uint64
	StructFields(bucket uint8) []ydb.StructValueOption
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
