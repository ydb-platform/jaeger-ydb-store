package log

import (
	"github.com/hashicorp/go-hclog"
	"github.com/spf13/viper"
	"github.com/ydb-platform/jaeger-ydb-store/internal/db"
)

func NewLogger(v *viper.Viper) (hclog.Logger, error) {
	output, err := newStderrFileWriter(v.GetString(db.KeyPluginLogPath))
	if err != nil {
		return nil, err
	}

	logger := hclog.New(&hclog.LoggerOptions{
		Name:       "ydb-store-plugin",
		Output:     output,
		JSONFormat: true,
	})

	return logger, nil
}
