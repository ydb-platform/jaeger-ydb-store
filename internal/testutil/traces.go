package testutil

import (
	"math/rand"
	"time"

	"github.com/jaegertracing/jaeger/model"
)

var r = rand.New(rand.NewSource(time.Now().UnixNano()))

func GenerateTraceID() model.TraceID {
	return model.TraceID{High: r.Uint64(), Low: r.Uint64()}
}
