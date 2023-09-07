package testutil

import (
	"github.com/jaegertracing/jaeger/pkg/jtracer"
	"go.opentelemetry.io/otel/trace"
)

func Tracer() trace.Tracer {
	return jtracer.NoOp().OTEL.Tracer("")
}
