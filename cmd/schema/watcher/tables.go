package watcher

import (
	"github.com/spf13/viper"

	"github.com/yandex-cloud/jaeger-ydb-store/schema"
)

type partDefinition struct {
	defFunc schema.PartitionedDefinition
	count   uint64
}

func definitions() map[string]partDefinition {
	m := make(map[string]partDefinition, len(schema.PartitionTables))
	for name, f := range schema.PartitionTables {
		m[name] = partDefinition{f, viper.GetUint64("parts_" + name)}
	}
	return m
}
