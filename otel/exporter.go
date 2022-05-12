package otel

import (
	"context"
	"strings"

	"github.com/jaegertracing/jaeger/storage/spanstore"
	jt "github.com/open-telemetry/opentelemetry-collector-contrib/pkg/translator/jaeger"
	"github.com/spf13/viper"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/yandex-cloud/jaeger-ydb-store/plugin"
)

func createTracesExporter(_ context.Context, settings component.ExporterCreateSettings, cfg config.Exporter) (component.TracesExporter, error) {
	v := viper.New()
	v.SetEnvKeyReplacer(strings.NewReplacer("-", "_", ".", "_"))
	v.AutomaticEnv()
	v.SetDefault("ydb", cfg.(*factoryConfig).YDBConfig)

	ydbPlugin := plugin.NewYdbStorage()
	ydbPlugin.InitFromViper(v)
	exp := &traceExporter{w: ydbPlugin.SpanWriter()}
	return exporterhelper.NewTracesExporter(cfg, settings, exp.push)
}

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
