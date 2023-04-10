package schema

import (
	"github.com/spf13/viper"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"

	"github.com/ydb-platform/jaeger-ydb-store/internal/db"
)

// Definition is a list of create table options
type (
	Definition            func() []options.CreateTableOption
	PartitionedDefinition func(partitionCount uint64) []options.CreateTableOption
)

var (
	// Tables are global tables
	Tables = map[string]Definition{
		"partitions":         Partitions,
		"service_names":      ServiceNames,
		"operation_names_v2": OperationNamesV2,
		"archive":            ArchiveTraces,
	}

	// PartitionTables tables split by partition
	PartitionTables = map[string]PartitionedDefinition{
		"traces":           Traces,
		"idx_service_name": ServiceNameIndex,
		"idx_service_op":   ServiceOperationIndex,
		"idx_duration":     DurationIndex,
		"idx_tag_v2":       TagIndexV2,
	}
)

// Traces returns traces table schema
func Traces(numPartitions uint64) []options.CreateTableOption {
	return append(
		ArchiveTraces(),
		options.WithProfile(
			options.WithPartitioningPolicy(options.WithPartitioningPolicyUniformPartitions(numPartitions)),
		),
		options.WithPartitioningSettingsObject(partitioningSettings(numPartitions)),
	)
}

// ArchiveTraces returns archive_traces table schema
func ArchiveTraces() []options.CreateTableOption {
	res := []options.CreateTableOption{
		options.WithColumn("trace_id_low", types.Optional(types.TypeUint64)),
		options.WithColumn("trace_id_high", types.Optional(types.TypeUint64)),
		options.WithColumn("span_id", types.Optional(types.TypeUint64)),
		options.WithColumn("operation_name", types.Optional(types.TypeUTF8)),
		options.WithColumn("flags", types.Optional(types.TypeUint32)),
		options.WithColumn("start_time", types.Optional(types.TypeInt64)),
		options.WithColumn("duration", types.Optional(types.TypeInt64)),
		options.WithColumn("extra", types.Optional(types.TypeString)),
		options.WithPrimaryKeyColumn("trace_id_low", "trace_id_high", "span_id"),
	}
	if viper.GetBool(db.KeyYDBFeatureCompression) {
		res = append(res,
			options.WithColumnFamilies(
				options.ColumnFamily{
					Name:        "default",
					Compression: options.ColumnFamilyCompressionLZ4,
				},
			),
		)
	}
	return res
}

// ServiceOperationIndex returns service_operation_index table schema
func ServiceOperationIndex(numPartitions uint64) []options.CreateTableOption {
	return []options.CreateTableOption{
		options.WithColumn("idx_hash", types.Optional(types.TypeUint64)),
		options.WithColumn("rev_start_time", types.Optional(types.TypeInt64)),
		options.WithColumn("uniq", types.Optional(types.TypeUint32)),
		options.WithColumn("trace_ids", types.Optional(types.TypeString)),
		options.WithPrimaryKeyColumn("idx_hash", "rev_start_time", "uniq"),
		options.WithProfile(
			options.WithPartitioningPolicy(options.WithPartitioningPolicyUniformPartitions(numPartitions)),
		),
		options.WithPartitioningSettingsObject(partitioningSettings(numPartitions)),
	}
}

// ServiceNameIndex returns service_name_index table schema
func ServiceNameIndex(numPartitions uint64) []options.CreateTableOption {
	return []options.CreateTableOption{
		options.WithColumn("idx_hash", types.Optional(types.TypeUint64)),
		options.WithColumn("rev_start_time", types.Optional(types.TypeInt64)),
		options.WithColumn("uniq", types.Optional(types.TypeUint32)),
		options.WithColumn("trace_ids", types.Optional(types.TypeString)),
		options.WithPrimaryKeyColumn("idx_hash", "rev_start_time", "uniq"),
		options.WithProfile(
			options.WithPartitioningPolicy(options.WithPartitioningPolicyUniformPartitions(numPartitions)),
		),
		options.WithPartitioningSettingsObject(partitioningSettings(numPartitions)),
	}
}

// DurationIndex returns duration_index table schema
func DurationIndex(numPartitions uint64) []options.CreateTableOption {
	return []options.CreateTableOption{
		options.WithColumn("idx_hash", types.Optional(types.TypeUint64)),
		options.WithColumn("duration", types.Optional(types.TypeInt64)),
		options.WithColumn("rev_start_time", types.Optional(types.TypeInt64)),
		options.WithColumn("uniq", types.Optional(types.TypeUint32)),
		options.WithColumn("trace_ids", types.Optional(types.TypeString)),
		options.WithPrimaryKeyColumn("idx_hash", "duration", "rev_start_time", "uniq"),
		options.WithProfile(
			options.WithPartitioningPolicy(options.WithPartitioningPolicyUniformPartitions(numPartitions)),
		),
		options.WithPartitioningSettingsObject(partitioningSettings(numPartitions)),
	}
}

// TagIndexV2 returns tag_index_v2 table schema
func TagIndexV2(numPartitions uint64) []options.CreateTableOption {
	return []options.CreateTableOption{
		options.WithColumn("idx_hash", types.Optional(types.TypeUint64)),
		options.WithColumn("rev_start_time", types.Optional(types.TypeInt64)),
		options.WithColumn("op_hash", types.Optional(types.TypeUint64)),
		options.WithColumn("uniq", types.Optional(types.TypeUint32)),
		options.WithColumn("trace_ids", types.Optional(types.TypeString)),
		options.WithPrimaryKeyColumn("idx_hash", "rev_start_time", "op_hash", "uniq"),
		options.WithProfile(
			options.WithPartitioningPolicy(options.WithPartitioningPolicyUniformPartitions(numPartitions)),
		),
		options.WithPartitioningSettingsObject(partitioningSettings(numPartitions)),
	}
}

// ServiceNames returns service_names table schema
func ServiceNames() []options.CreateTableOption {
	return []options.CreateTableOption{
		options.WithColumn("service_name", types.Optional(types.TypeUTF8)),
		options.WithPrimaryKeyColumn("service_name"),
	}
}

// OperationNamesV2 returns operation_names_v2 table schema
func OperationNamesV2() []options.CreateTableOption {
	return []options.CreateTableOption{
		options.WithColumn("service_name", types.Optional(types.TypeUTF8)),
		options.WithColumn("span_kind", types.Optional(types.TypeUTF8)),
		options.WithColumn("operation_name", types.Optional(types.TypeUTF8)),
		options.WithPrimaryKeyColumn("service_name", "span_kind", "operation_name"),
	}
}

func Partitions() []options.CreateTableOption {
	return []options.CreateTableOption{
		options.WithColumn("part_date", types.Optional(types.TypeUTF8)),
		options.WithColumn("part_num", types.Optional(types.TypeUint8)),
		options.WithColumn("is_active", types.Optional(types.TypeBool)),
		options.WithPrimaryKeyColumn("part_date", "part_num"),
	}
}

func partitioningSettings(numPartitions uint64) (settings options.PartitioningSettings) {
	settings = options.PartitioningSettings{
		PartitioningBySize: options.FeatureEnabled,
		PartitionSizeMb:    uint64(viper.GetSizeInBytes(db.KeyYDBPartitionSize) / 1024 / 1024),
		MinPartitionsCount: numPartitions,
	}
	if viper.GetBool(db.KeyYDBFeatureSplitByLoad) {
		settings.PartitioningByLoad = options.FeatureEnabled
	}
	return
}
