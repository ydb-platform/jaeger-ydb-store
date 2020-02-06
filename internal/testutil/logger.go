package testutil

import (
	"go.uber.org/zap"
	"os"
)

func Zap() *zap.Logger {
	if os.Getenv("TEST_LOGGING") == "1" {
		logger, _ := zap.NewDevelopment()
		return logger
	}
	return zap.NewNop()
}
