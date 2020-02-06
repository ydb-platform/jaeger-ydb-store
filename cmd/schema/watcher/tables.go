package watcher

import (
	"github.com/YandexClassifieds/jaeger-ydb-store/schema"
	"github.com/spf13/viper"
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
