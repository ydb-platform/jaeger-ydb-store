package otel

import (
	"context"

	"github.com/jaegertracing/jaeger/storage/spanstore"
	jt "github.com/open-telemetry/opentelemetry-collector-contrib/pkg/translator/jaeger"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

type traceExporter struct {
	w spanstore.Writer
}

func (t *traceExporter) push(ctx context.Context, td ptrace.Traces) error {
	batches, err := jt.ProtoFromTraces(td)
	if err != nil {
		return err
	}
	for _, b := range batches {
		for _, span := range b.GetSpans() {
			if span.Process == nil {
				span.Process = b.Process
			}
			err = t.w.WriteSpan(ctx, span)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
