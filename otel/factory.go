package otel

import (
	"context"
	"strings"

	"github.com/spf13/viper"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config"
	"go.opentelemetry.io/collector/exporter/exporterhelper"

	"github.com/yandex-cloud/jaeger-ydb-store/plugin"
)

const (
	typeVal = "ydb"
)

func NewFactory() component.ExporterFactory {
	return component.NewExporterFactory(typeVal, func() config.Exporter {
		return &Config{
			ExporterSettings: config.NewExporterSettings(config.NewComponentID(typeVal)),
			TimeoutSettings:  exporterhelper.NewDefaultTimeoutSettings(),
			QueueSettings:    exporterhelper.QueueSettings{Enabled: false},
			RetrySettings:    exporterhelper.NewDefaultRetrySettings(),
		}
	}, component.WithTracesExporter(createTracesExporter))
}

func createTracesExporter(_ context.Context, set component.ExporterCreateSettings, cfg config.Exporter) (component.TracesExporter, error) {
	v := viper.New()
	v.SetEnvKeyReplacer(strings.NewReplacer("-", "_", ".", "_"))
	v.AutomaticEnv()
	v.SetDefault("ydb", cfg.(*Config).YDBConfig)
	typedCfg := cfg.(*Config)

	ydbPlugin := plugin.NewYdbStorage()
	ydbPlugin.InitFromViper(v)
	exp := &traceExporter{w: ydbPlugin.SpanWriter()}
	return exporterhelper.NewTracesExporter(
		cfg,
		set,
		exp.push,
		exporterhelper.WithRetry(typedCfg.RetrySettings),
		exporterhelper.WithQueue(typedCfg.QueueSettings),
		exporterhelper.WithTimeout(typedCfg.TimeoutSettings),
	)
}
