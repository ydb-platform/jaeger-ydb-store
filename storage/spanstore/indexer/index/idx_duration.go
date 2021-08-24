package index

import (
	"bytes"
	"encoding/binary"
	"time"

	"github.com/dgryski/go-farm"
	"github.com/jaegertracing/jaeger/model"
	"github.com/yandex-cloud/ydb-go-sdk/v2"

	"github.com/yandex-cloud/jaeger-ydb-store/storage/spanstore/dbmodel"
)

func DurationIndexValue(d time.Duration) int64 {
	switch {
	case d < time.Millisecond*100:
		return int64(d.Truncate(time.Millisecond * 10))
	case d < time.Second:
		return int64(d.Truncate(time.Millisecond * 100))
	default:
		return int64(d.Truncate(time.Second / 2))
	}
}

type durationIndex struct {
	baseIndex
	serviceName   string
	operationName string
	duration      int64
}

func NewDurationIndex(span *model.Span, opName string) Indexable {
	return durationIndex{
		baseIndex:     newBaseIndex(span),
		serviceName:   span.Process.ServiceName,
		operationName: opName,
		duration:      DurationIndexValue(span.Duration),
	}
}

func (i durationIndex) Hash() uint64 {
	buf := new(bytes.Buffer)
	buf.WriteString(i.serviceName)
	buf.WriteString(i.operationName)
	_ = binary.Write(buf, binary.BigEndian, i.duration)
	return farm.Hash64(buf.Bytes())
}

func (i durationIndex) StructFields(bucket uint8) []ydb.StructValueOption {
	return []ydb.StructValueOption{
		ydb.StructFieldValue("idx_hash", ydb.Uint64Value(dbmodel.HashBucketData(bucket, i.serviceName, i.operationName))),
		ydb.StructFieldValue("duration", ydb.Int64Value(i.duration)),
		ydb.StructFieldValue("rev_start_time", ydb.Int64Value(-i.startTime.UnixNano())),
	}
}
