package testutil

import (
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
