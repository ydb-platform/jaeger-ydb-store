package testutil

import (
	"github.com/hashicorp/go-hclog"
	"os"

	"go.uber.org/zap"
)

func Zap() *zap.Logger {
	if os.Getenv("TEST_LOGGING") == "1" {
		logger, _ := zap.NewDevelopment()
		return logger
	}
	return zap.NewNop()
}

func JaegerLogger() hclog.Logger {
	return hclog.NewNullLogger()
}
