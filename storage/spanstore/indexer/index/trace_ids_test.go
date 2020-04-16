package index

import (
	"testing"

	"github.com/jaegertracing/jaeger/model"
	"github.com/stretchr/testify/assert"
)

func TestTraceIDList_ToBytes(t *testing.T) {
	l := TraceIDList{
		model.NewTraceID(1, 2),
		model.NewTraceID(2, 3),
		model.NewTraceID(4, 5),
	}
	result := l.ToBytes()
	assert.Len(t, result, 48)

	resultList, err := TraceIDListFromBytes(result)
	if !assert.NoError(t, err) {
		return
	}
	assert.Equal(t, resultList, l)
}
