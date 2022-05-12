package otel

import (
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config"
)

const (
	typeVal = "ydb"
)

type factoryConfig struct {
	config.ExporterSettings
	YDBConfig map[string]interface{} `mapstructure:",remain"`
}

func NewFactory() component.ExporterFactory {
	return component.NewExporterFactory(typeVal, func() config.Exporter {
		return &factoryConfig{
			ExporterSettings: config.NewExporterSettings(config.NewComponentID(typeVal)),
		}
	}, component.WithTracesExporter(createTracesExporter))
}
