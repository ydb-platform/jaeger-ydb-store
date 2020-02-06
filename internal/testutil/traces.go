package testutil

import (
	"github.com/jaegertracing/jaeger/model"
	"math/rand"
	"time"
)

var (
	r = rand.New(rand.NewSource(time.Now().UnixNano()))
)

func GenerateTraceID() model.TraceID {
	return model.TraceID{High: r.Uint64(), Low: r.Uint64()}
}
