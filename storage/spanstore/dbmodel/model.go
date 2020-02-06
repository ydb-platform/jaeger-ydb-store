package dbmodel

import (
	"github.com/gogo/protobuf/proto"
	"github.com/jaegertracing/jaeger/model"
	"github.com/pkg/errors"
	"time"
)

// Span represents db-serializable model
type Span struct {
	TraceIDLow    uint64
	TraceIDHigh   uint64
	SpanID        uint64
	OperationName string
	Flags         uint32
	StartTime     int64
	Duration      int64
	Extra         []byte
}

// FromDomain converts plugin model to db model or returns error
func FromDomain(span *model.Span) (*Span, error) {
	spanData := SpanData{
		Process:    span.Process,
		Tags:       span.Tags,
		Logs:       span.Logs,
		References: span.References,
	}
	extra, err := proto.Marshal(&spanData)
	if err != nil {
		return nil, errors.Wrap(err, "marshal err")
	}
	dbSpan := &Span{
		TraceIDHigh:   span.TraceID.High,
		TraceIDLow:    span.TraceID.Low,
		SpanID:        uint64(span.SpanID),
		OperationName: span.OperationName,
		Flags:         uint32(span.Flags),
		StartTime:     span.StartTime.UnixNano(),
		Duration:      int64(span.Duration),
		Extra:         extra,
	}
	return dbSpan, nil
}

// ToDomain converts db model to plugin model
func ToDomain(dbSpan *Span) (*model.Span, error) {
	spanData := SpanData{}
	err := proto.Unmarshal(dbSpan.Extra, &spanData)
	if err != nil {
		return nil, errors.Wrap(err, "dbSpan.Extra unmarshal error")

	}

	span := &model.Span{
		TraceID:       model.NewTraceID(dbSpan.TraceIDHigh, dbSpan.TraceIDLow),
		SpanID:        model.SpanID(dbSpan.SpanID),
		OperationName: dbSpan.OperationName,
		Flags:         model.Flags(dbSpan.Flags),
		StartTime:     time.Unix(0, dbSpan.StartTime).UTC(),
		Duration:      time.Duration(dbSpan.Duration),
		Process:       spanData.Process,
		References:    spanData.References,
		Tags:          spanData.Tags,
		Logs:          spanData.Logs,
	}
	return span, nil
}
