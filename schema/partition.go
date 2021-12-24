package schema

import (
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

const (
	partitionDateFormat = "20060102"
)

var (
	numPartitions = 10
	partitionStep = time.Hour * 24 / 10
)

func init() {
	// TODO: should probably rewrite partition helper funcs as some sort of schema builder object
	if v, err := strconv.Atoi(os.Getenv("YDB_SCHEMA_NUM_PARTITIONS")); err == nil {
		numPartitions = v
		partitionStep = time.Hour * 24 / time.Duration(numPartitions)
	}
}

type PartitionKey struct {
	Date     string
	Num      uint8
	IsActive bool
}

func (k PartitionKey) Suffix() string {
	w := new(strings.Builder)
	w.WriteString(k.Date)
	w.WriteString("_")
	w.WriteString(strconv.FormatInt(int64(k.Num), 10))
	return w.String()
}

func (k PartitionKey) QueryWhereParams() *table.QueryParameters {
	return table.NewQueryParameters(
		table.ValueParam("$part_date", types.UTF8Value(k.Date)),
		table.ValueParam("$part_num", types.Uint8Value(uint8(k.Num))),
	)
}

func (k PartitionKey) QueryParams() *table.QueryParameters {
	return table.NewQueryParameters(
		table.ValueParam("$part_date", types.UTF8Value(k.Date)),
		table.ValueParam("$part_num", types.Uint8Value(uint8(k.Num))),
		table.ValueParam("$is_active", types.BoolValue(k.IsActive)),
	)
}

func (k PartitionKey) BuildFullTableName(dbPath, table string) string {
	sb := new(strings.Builder)
	sb.WriteString(dbPath)
	sb.WriteString("/")
	sb.WriteString(table)
	sb.WriteString("_")
	sb.WriteString(k.Date)
	sb.WriteString("_")
	sb.WriteString(strconv.FormatInt(int64(k.Num), 10))
	return sb.String()
}

func (k PartitionKey) TimeSpan() (begin time.Time, end time.Time) {
	t, err := time.Parse(partitionDateFormat, k.Date)
	if err != nil {
		return
	}
	begin = t.Add(time.Duration(k.Num) * partitionStep)
	end = t.Add(time.Duration(k.Num+1) * partitionStep)
	return
}

func PartitionFromTime(t time.Time) PartitionKey {
	hours := t.UTC().Sub(t.Truncate(time.Hour * 24)).Hours()
	return PartitionKey{
		Date:     t.UTC().Format(partitionDateFormat),
		Num:      uint8(hours * float64(numPartitions) / 24),
		IsActive: true,
	}
}

func MakePartitionList(start, end time.Time) []PartitionKey {
	cur := start.Truncate(partitionStep)
	n := int(end.Sub(cur)/partitionStep) + 1
	retMe := make([]PartitionKey, 0, n)
	for end.After(cur) || end.Equal(cur) {
		retMe = append(retMe, PartitionFromTime(cur))
		cur = cur.Add(partitionStep)
	}
	return retMe
}

func IntersectPartList(a, b []PartitionKey) []PartitionKey {
	hash := make(map[PartitionKey]struct{}, len(a))
	set := make([]PartitionKey, 0, int(math.Min(float64(len(a)), float64(len(b)))))

	for _, el := range a {
		hash[el] = struct{}{}
	}
	for _, el := range b {
		if _, found := hash[el]; found {
			set = append(set, el)
		}
	}
	return set
}
