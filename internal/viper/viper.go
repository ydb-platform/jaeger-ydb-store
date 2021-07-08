package viper

import (
	"log"
	"os"
	"path/filepath"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

// ConfigureViperFromFlag reads --config flag and attempts to setup v.
func ConfigureViperFromFlag(v *viper.Viper) {
	var cmd = pflag.NewFlagSet(os.Args[0], pflag.ExitOnError)
	var path = cmd.String("config", "", "full path to configuration file")
	// Ignore errors; cmd is set for ExitOnError.
	_ = cmd.Parse(os.Args[1:])

	if len(*path) > 0 {
		var extension = filepath.Ext(*path)
		if len(extension) == 0 {
			return
		}
		extension = extension[1:]
		v.SetConfigType(extension)

		f, err := os.Open(*path)
		if err != nil {
			log.Fatal("Could not open file", *path)
		}
		err = v.ReadConfig(f)
		if err != nil {
			log.Fatal("Could not read config file", *path)
		}
	}
}
