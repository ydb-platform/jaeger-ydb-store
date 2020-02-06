package schema

import "fmt"

var (
	queryPartitions       = "SELECT part_date, part_num, is_active FROM `%s`"
	queryActivePartitions = "SELECT part_date, part_num, is_active FROM `%s` WHERE is_active=true"
	deletePartitionQ      = `DECLARE $part_date as Utf8; DECLARE $part_num as Uint8;
DELETE FROM ` + "`%s`" + ` WHERE part_date = $part_date AND part_num = $part_num
`
	insertPartitionQ = `DECLARE $part_date as Utf8;
DECLARE $part_num as Uint8;
DECLARE $is_active as Bool;
UPSERT INTO ` + "`%s`" + ` (part_date, part_num, is_active) VALUES ($part_date, $part_num, $is_active)`

	updatePartitionQ = `DECLARE $part_date as Utf8;
DECLARE $part_num as Uint8;
DECLARE $is_active as Bool;
UPDATE ` + "`%s`" + ` SET is_active = $is_active WHERE part_date = $part_date AND part_num = $part_num`

	m = map[QueryName]queryInfo{
		QueryParts:       {"partitions", queryPartitions},
		QueryActiveParts: {"partitions", queryActivePartitions},
		DeletePart:       {"partitions", deletePartitionQ},
		InsertPart:       {"partitions", insertPartitionQ},
		UpdatePart:       {"partitions", updatePartitionQ},
		DeleteAllParts:   {"partitions", "DELETE FROM `%s`"},
	}
)

type QueryName int

const (
	QueryParts QueryName = iota
	QueryActiveParts
	DeletePart
	InsertPart
	UpdatePart
	DeleteAllParts
)

type queryInfo struct {
	table string
	query string
}

func BuildQuery(p DbPath, queryName QueryName) string {
	if i, ok := m[queryName]; ok {
		return fmt.Sprintf(i.query, p.Table(i.table))
	}
	panic("query not found")
}
