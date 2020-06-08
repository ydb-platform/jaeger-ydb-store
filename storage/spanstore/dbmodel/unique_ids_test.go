package dbmodel

import (
	"github.com/jaegertracing/jaeger/model"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestIntersectTraceIDs(t *testing.T) {
	a := NewUniqueTraceIDs()
	a.Add(TraceIDFromDomain(model.NewTraceID(1, 1)))
	a.Add(TraceIDFromDomain(model.NewTraceID(1, 2)))
	a.Add(TraceIDFromDomain(model.NewTraceID(1, 3)))
	b := NewUniqueTraceIDs()
	b.Add(TraceIDFromDomain(model.NewTraceID(1, 2)))
	b.Add(TraceIDFromDomain(model.NewTraceID(1, 3)))
	b.Add(TraceIDFromDomain(model.NewTraceID(1, 4)))

	result := IntersectTraceIDs([]*UniqueTraceIDs{a, b}).AsList()
	expected := []TraceID{TraceIDFromDomain(model.NewTraceID(1, 2)), TraceIDFromDomain(model.NewTraceID(1, 3))}
	assert.Equal(t, expected, result)
}
