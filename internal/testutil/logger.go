package testutil

import (
	"os"

	"github.com/hashicorp/go-hclog"
)

func Hclog() hclog.Logger {
	if os.Getenv("TEST_LOGGING") == "1" {
		return hclog.Default()
	}
	return hclog.NewNullLogger()
}
