package dbmodel

import (
	"sort"
	"strings"
	"testing"

	"github.com/jaegertracing/jaeger/model"
	"github.com/stretchr/testify/assert"
)

func sortTraceIDs(traceIDs []TraceID) []TraceID {
	sort.Slice(traceIDs, func(i, j int) bool {
		return strings.Compare(string(traceIDs[i][:]), string(traceIDs[j][:])) < 0
	})
	return traceIDs
}

func TestIntersectTraceIDs(t *testing.T) {
	a := NewUniqueTraceIDs()
	a.Add(TraceIDFromDomain(model.NewTraceID(1, 1)))
	a.Add(TraceIDFromDomain(model.NewTraceID(1, 2)))
	a.Add(TraceIDFromDomain(model.NewTraceID(1, 3)))
	b := NewUniqueTraceIDs()
	b.Add(TraceIDFromDomain(model.NewTraceID(1, 2)))
	b.Add(TraceIDFromDomain(model.NewTraceID(1, 3)))
	b.Add(TraceIDFromDomain(model.NewTraceID(1, 4)))

	result := sortTraceIDs(IntersectTraceIDs([]*UniqueTraceIDs{a, b}).AsList())
	expected := sortTraceIDs([]TraceID{TraceIDFromDomain(model.NewTraceID(1, 2)), TraceIDFromDomain(model.NewTraceID(1, 3))})
	assert.Equal(t, expected, result)
}
