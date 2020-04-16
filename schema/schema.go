package schema

import (
	"github.com/yandex-cloud/ydb-go-sdk"
	"github.com/yandex-cloud/ydb-go-sdk/table"
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
	}

	// PartitionTables tables split by partition
	PartitionTables = map[string]PartitionedDefinition{
		"traces":           Traces,
		"idx_service_name": ServiceNameIndex,
		"idx_service_op":   ServiceOperationIndex,
		"idx_duration":     DurationIndex,
		"idx_tag":          TagIndex,
	}
)

// Traces returns traces table schema
func Traces(numPartitions uint64) []table.CreateTableOption {
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
		table.WithProfile(
			table.WithPartitioningPolicy(
				table.WithPartitioningPolicyUniformPartitions(numPartitions),
			),
		),
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
		table.WithProfile(table.WithPartitioningPolicy(table.WithPartitioningPolicyUniformPartitions(numPartitions))),
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
		table.WithProfile(table.WithPartitioningPolicy(table.WithPartitioningPolicyUniformPartitions(numPartitions))),
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
		table.WithProfile(table.WithPartitioningPolicy(table.WithPartitioningPolicyUniformPartitions(numPartitions))),
	}
}

// TagIndex returns tag_index table schema
func TagIndex(numPartitions uint64) []table.CreateTableOption {
	return []table.CreateTableOption{
		table.WithColumn("idx_hash", ydb.Optional(ydb.TypeUint64)),
		table.WithColumn("rev_start_time", ydb.Optional(ydb.TypeInt64)),
		table.WithColumn("uniq", ydb.Optional(ydb.TypeUint32)),
		table.WithColumn("trace_ids", ydb.Optional(ydb.TypeString)),
		table.WithPrimaryKeyColumn("idx_hash", "rev_start_time", "uniq"),
		table.WithProfile(table.WithPartitioningPolicy(table.WithPartitioningPolicyUniformPartitions(numPartitions))),
	}
}

// ServiceNames returns service_names table schema
func ServiceNames() []table.CreateTableOption {
	return []table.CreateTableOption{
		table.WithColumn("service_name", ydb.Optional(ydb.TypeUTF8)),
		table.WithPrimaryKeyColumn("service_name"),
	}
}

// OperationNames returns operation_names_v2 table schema
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
