package testutil

import (
	"github.com/hashicorp/go-hclog"
	"os"
)

func Hclog() hclog.Logger {
	if os.Getenv("TEST_LOGGING") == "1" {
		return hclog.Default()
	}
	return hclog.NewNullLogger()
}
