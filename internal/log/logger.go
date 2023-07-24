package log

import (
	"github.com/hashicorp/go-hclog"
	"github.com/spf13/viper"
)

func NewLogger(v *viper.Viper) (hclog.Logger, error) {
	output, err := newCustomWriter(v.GetString("plugin_log_path"))
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
