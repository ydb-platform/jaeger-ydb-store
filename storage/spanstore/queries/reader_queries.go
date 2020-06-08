package queries

import (
	"fmt"
	"strings"

	"github.com/yandex-cloud/jaeger-ydb-store/schema"
)

const (
	queryByTraceID = `DECLARE $trace_id_high AS uint64;
DECLARE $trace_id_low AS uint64;
DECLARE $limit AS uint64;
DECLARE $offset as uint64;
SELECT trace_id_high, trace_id_low, span_id, operation_name, flags, start_time, duration, extra
FROM ` + "`%s`" + `
WHERE trace_id_high = $trace_id_high and trace_id_low = $trace_id_low
LIMIT $offset,$limit`

	querySpanCount = `DECLARE $trace_id_high AS uint64;
		DECLARE $trace_id_low AS uint64;
		SELECT COUNT(*) AS c
		FROM ` + "`%s`" + `
		WHERE trace_id_high = $trace_id_high and trace_id_low = $trace_id_low
`

	queryByTag = `DECLARE $hash AS uint64;
DECLARE $time_min AS int64;
DECLARE $time_max AS int64;
DECLARE $limit AS uint64;
SELECT trace_ids, rev_start_time
FROM ` + "`%s`" + `
WHERE idx_hash = $hash AND rev_start_time <= 0-$time_min AND rev_start_time >= 0-$time_max
LIMIT $limit`

	queryByServiceAndOperationName = `DECLARE $idx_hash AS uint64;
DECLARE $time_min AS int64;
DECLARE $time_max AS int64;
DECLARE $limit AS uint64;
SELECT trace_ids, rev_start_time
FROM ` + "`%s`" + `
WHERE idx_hash = $idx_hash
AND rev_start_time <= 0-$time_min AND rev_start_time >= 0-$time_max
LIMIT $limit`

	queryByDuration = `DECLARE $hash AS uint64;
DECLARE $duration_min AS int64;
DECLARE $duration_max AS int64;
DECLARE $time_min AS int64;
DECLARE $time_max AS int64;
DECLARE $limit AS uint64;
SELECT trace_ids, rev_start_time
FROM ` + "`%s`" + `
WHERE idx_hash = $hash AND rev_start_time <= 0-$time_min AND rev_start_time >= 0-$time_max
AND duration >= $duration_min AND duration <= $duration_max
LIMIT $limit`

	queryByServiceName = `DECLARE $idx_hash AS uint64;
DECLARE $time_min AS int64;
DECLARE $time_max AS int64;
DECLARE $limit AS uint64;
SELECT trace_ids, rev_start_time
FROM ` + "`%s`" + `
WHERE idx_hash = $idx_hash
AND rev_start_time <= 0-$time_min AND rev_start_time >= 0-$time_max
LIMIT $limit`

	queryServiceNames = `SELECT service_name FROM ` + "`%s`"
	queryOperations   = `DECLARE $service_name AS utf8;
SELECT operation_name FROM ` + "`%s`" + ` WHERE service_name = $service_name`
	queryOperationsWithKind = `DECLARE $service_name AS utf8;
DECLARE $span_kind AS utf8;
SELECT operation_name FROM ` + "`%s`" + ` WHERE service_name = $service_name
AND span_kind = $span_kind`
)

var (
	m = map[string]queryInfo{
		"query-services":             {"service_names", queryServiceNames},
		"query-operations":           {"operation_names_v2", queryOperations},
		"query-operations-with-kind": {"operation_names_v2", queryOperationsWithKind},
	}

	pm = map[string]queryInfo{
		"queryByTraceID":                 {"traces", queryByTraceID},
		"querySpanCount":                 {"traces", querySpanCount},
		"queryByTag":                     {"idx_tag", queryByTag},
		"queryByDuration":                {"idx_duration", queryByDuration},
		"queryByServiceAndOperationName": {"idx_service_op", queryByServiceAndOperationName},
		"queryByServiceName":             {"idx_service_name", queryByServiceName},
	}
)

type queryInfo struct {
	table string
	query string
}

func BuildQuery(queryName string, path schema.DbPath) string {
	if i, ok := m[queryName]; ok {
		return fmt.Sprintf(i.query, path.FullTable(i.table))
	}
	panic("query not found")
}

func BuildPartitionQuery(queryName string, path schema.DbPath, part schema.PartitionKey) string {
	if i, ok := pm[queryName]; ok {
		ft := new(strings.Builder)
		ft.WriteString(path.Table(i.table))
		ft.WriteString("_")
		ft.WriteString(part.Suffix())
		return fmt.Sprintf(i.query, ft.String())
	}
	panic("query not found")
}
