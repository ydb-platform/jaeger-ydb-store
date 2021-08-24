package schema

import (
	"github.com/spf13/viper"
	"github.com/yandex-cloud/jaeger-ydb-store/internal/db"
	"github.com/yandex-cloud/ydb-go-sdk/v2"
	"github.com/yandex-cloud/ydb-go-sdk/v2/table"
)

// Definition is a list of create table options
type Definition func() []table.CreateTableOption
type PartitionedDefinition func(partitionCount uint64) []table.CreateTableOption

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
func Traces(numPartitions uint64) []table.CreateTableOption {
	return append(
		ArchiveTraces(),
		table.WithProfile(
			table.WithPartitioningPolicy(table.WithPartitioningPolicyUniformPartitions(numPartitions)),
		),
		table.WithPartitioningSettingsObject(partitioningSettings(numPartitions)),
	)
}

// ArchiveTraces returns archive_traces table schema
func ArchiveTraces() []table.CreateTableOption {
	res := []table.CreateTableOption{
		table.WithColumn("trace_id_low", ydb.Optional(ydb.TypeUint64)),
		table.WithColumn("trace_id_high", ydb.Optional(ydb.TypeUint64)),
		table.WithColumn("span_id", ydb.Optional(ydb.TypeUint64)),
		table.WithColumn("operation_name", ydb.Optional(ydb.TypeUTF8)),
		table.WithColumn("flags", ydb.Optional(ydb.TypeUint32)),
		table.WithColumn("start_time", ydb.Optional(ydb.TypeInt64)),
		table.WithColumn("duration", ydb.Optional(ydb.TypeInt64)),
		table.WithColumn("extra", ydb.Optional(ydb.TypeString)),
		table.WithPrimaryKeyColumn("trace_id_low", "trace_id_high", "span_id"),
	}
	if viper.GetBool(db.KeyYDBFeatureCompression) {
		res = append(res,
			table.WithColumnFamilies(
				table.ColumnFamily{
					Name:        "default",
					Compression: table.ColumnFamilyCompressionLZ4,
				},
			),
		)
	}
	return res
}

// ServiceOperationIndex returns service_operation_index table schema
func ServiceOperationIndex(numPartitions uint64) []table.CreateTableOption {
	return []table.CreateTableOption{
		table.WithColumn("idx_hash", ydb.Optional(ydb.TypeUint64)),
		table.WithColumn("rev_start_time", ydb.Optional(ydb.TypeInt64)),
		table.WithColumn("uniq", ydb.Optional(ydb.TypeUint32)),
		table.WithColumn("trace_ids", ydb.Optional(ydb.TypeString)),
		table.WithPrimaryKeyColumn("idx_hash", "rev_start_time", "uniq"),
		table.WithProfile(
			table.WithPartitioningPolicy(table.WithPartitioningPolicyUniformPartitions(numPartitions)),
		),
		table.WithPartitioningSettingsObject(partitioningSettings(numPartitions)),
	}
}

// ServiceNameIndex returns service_name_index table schema
func ServiceNameIndex(numPartitions uint64) []table.CreateTableOption {
	return []table.CreateTableOption{
		table.WithColumn("idx_hash", ydb.Optional(ydb.TypeUint64)),
		table.WithColumn("rev_start_time", ydb.Optional(ydb.TypeInt64)),
		table.WithColumn("uniq", ydb.Optional(ydb.TypeUint32)),
		table.WithColumn("trace_ids", ydb.Optional(ydb.TypeString)),
		table.WithPrimaryKeyColumn("idx_hash", "rev_start_time", "uniq"),
		table.WithProfile(
			table.WithPartitioningPolicy(table.WithPartitioningPolicyUniformPartitions(numPartitions)),
		),
		table.WithPartitioningSettingsObject(partitioningSettings(numPartitions)),
	}
}

// DurationIndex returns duration_index table schema
func DurationIndex(numPartitions uint64) []table.CreateTableOption {
	return []table.CreateTableOption{
		table.WithColumn("idx_hash", ydb.Optional(ydb.TypeUint64)),
		table.WithColumn("duration", ydb.Optional(ydb.TypeInt64)),
		table.WithColumn("rev_start_time", ydb.Optional(ydb.TypeInt64)),
		table.WithColumn("uniq", ydb.Optional(ydb.TypeUint32)),
		table.WithColumn("trace_ids", ydb.Optional(ydb.TypeString)),
		table.WithPrimaryKeyColumn("idx_hash", "duration", "rev_start_time", "uniq"),
		table.WithProfile(
			table.WithPartitioningPolicy(table.WithPartitioningPolicyUniformPartitions(numPartitions)),
		),
		table.WithPartitioningSettingsObject(partitioningSettings(numPartitions)),
	}
}

// TagIndexV2 returns tag_index_v2 table schema
func TagIndexV2(numPartitions uint64) []table.CreateTableOption {
	return []table.CreateTableOption{
		table.WithColumn("idx_hash", ydb.Optional(ydb.TypeUint64)),
		table.WithColumn("rev_start_time", ydb.Optional(ydb.TypeInt64)),
		table.WithColumn("op_hash", ydb.Optional(ydb.TypeUint64)),
		table.WithColumn("uniq", ydb.Optional(ydb.TypeUint32)),
		table.WithColumn("trace_ids", ydb.Optional(ydb.TypeString)),
		table.WithPrimaryKeyColumn("idx_hash", "rev_start_time", "op_hash", "uniq"),
		table.WithProfile(
			table.WithPartitioningPolicy(table.WithPartitioningPolicyUniformPartitions(numPartitions)),
		),
		table.WithPartitioningSettingsObject(partitioningSettings(numPartitions)),
	}
}

// ServiceNames returns service_names table schema
func ServiceNames() []table.CreateTableOption {
	return []table.CreateTableOption{
		table.WithColumn("service_name", ydb.Optional(ydb.TypeUTF8)),
		table.WithPrimaryKeyColumn("service_name"),
	}
}

// OperationNamesV2 returns operation_names_v2 table schema
func OperationNamesV2() []table.CreateTableOption {
	return []table.CreateTableOption{
		table.WithColumn("service_name", ydb.Optional(ydb.TypeUTF8)),
		table.WithColumn("span_kind", ydb.Optional(ydb.TypeUTF8)),
		table.WithColumn("operation_name", ydb.Optional(ydb.TypeUTF8)),
		table.WithPrimaryKeyColumn("service_name", "span_kind", "operation_name"),
	}
}

func Partitions() []table.CreateTableOption {
	return []table.CreateTableOption{
		table.WithColumn("part_date", ydb.Optional(ydb.TypeUTF8)),
		table.WithColumn("part_num", ydb.Optional(ydb.TypeUint8)),
		table.WithColumn("is_active", ydb.Optional(ydb.TypeBool)),
		table.WithPrimaryKeyColumn("part_date", "part_num"),
	}
}

func partitioningSettings(numPartitions uint64) (settings table.PartitioningSettings) {
	settings = table.PartitioningSettings{
		PartitioningBySize: ydb.FeatureEnabled,
		PartitionSizeMb:    uint64(viper.GetSizeInBytes(db.KeyYDBPartitionSize) / 1024 / 1024),
		MinPartitionsCount: numPartitions,
	}
	if viper.GetBool(db.KeyYDBFeatureSplitByLoad) {
		settings.PartitioningByLoad = ydb.FeatureEnabled
	}
	return
}
