package otel

import (
	"fmt"

	"go.opentelemetry.io/collector/config"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

type Config struct {
	config.ExporterSettings        `mapstructure:",squash"` // squash ensures fields are correctly decoded in embedded struct
	exporterhelper.TimeoutSettings `mapstructure:",squash"` // squash ensures fields are correctly decoded in embedded struct.
	exporterhelper.QueueSettings   `mapstructure:"sending_queue"`
	exporterhelper.RetrySettings   `mapstructure:"retry_on_failure"`

	YDBConfig map[string]interface{} `mapstructure:",remain"`
}

// Validate checks if the exporter configuration is valid
func (cfg *Config) Validate() error {
	if err := cfg.QueueSettings.Validate(); err != nil {
		return fmt.Errorf("queue settings has invalid configuration: %w", err)
	}

	return nil
}
