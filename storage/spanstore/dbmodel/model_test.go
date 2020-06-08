package dbmodel

import (
	"github.com/jaegertracing/jaeger/model"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"

	"github.com/yandex-cloud/jaeger-ydb-store/internal/testutil"
)

func TestEncDec(t *testing.T) {
	span := &model.Span{
		TraceID:   testutil.GenerateTraceID(),
		SpanID:    model.NewSpanID(42),
		StartTime: time.Now().Round(0).UTC(),
		Process: model.NewProcess("svc1", []model.KeyValue{
			model.String("k", "v"),
			model.Int64("k2", 1),
		}),
		Tags: []model.KeyValue{
			model.String("kk", "vv"),
			model.Int64("a", 1),
		},
		References: []model.SpanRef{
			{SpanID: 1, TraceID: model.NewTraceID(42, 0)},
		},
		Logs: []model.Log{
			{
				Timestamp: time.Now().Round(0).UTC(),
				Fields:    []model.KeyValue{model.String("log", "record")},
			},
			{
				Timestamp: time.Now().Round(0).UTC(),
				Fields:    []model.KeyValue{model.String("log2", "record2")},
			},
		},
	}

	dbSpan, err := FromDomain(span)
	if !assert.NoError(t, err) {
		return
	}
	resultSpan, err := ToDomain(dbSpan)
	if !assert.NoError(t, err) {
		return
	}
	assert.Equal(t, span.StartTime, resultSpan.StartTime)
	assert.Equal(t, span, resultSpan)
}
