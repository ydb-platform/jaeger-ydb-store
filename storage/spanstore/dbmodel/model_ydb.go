package dbmodel

import (
	"github.com/yandex-cloud/ydb-go-sdk"
	"github.com/yandex-cloud/ydb-go-sdk/table"
	"strconv"
)

var (
	_ = strconv.Itoa
	_ = ydb.StringValue
	_ = table.NewQueryParameters
)

func (s *Span) Scan(res *table.Result) (err error) {
	res.SeekItem("trace_id_low")
	s.TraceIDLow = res.OUint64()

	res.SeekItem("trace_id_high")
	s.TraceIDHigh = res.OUint64()

	res.SeekItem("span_id")
	s.SpanID = res.OUint64()

	res.SeekItem("operation_name")
	s.OperationName = res.OUTF8()

	res.SeekItem("flags")
	s.Flags = res.OUint32()

	res.SeekItem("start_time")
	s.StartTime = res.OInt64()

	res.SeekItem("duration")
	s.Duration = res.OInt64()

	res.SeekItem("extra")
	s.Extra = res.OString()

	return res.Err()
}

func (s *Span) StructValue() ydb.Value {
	val := ydb.StructValue(
		ydb.StructFieldValue("trace_id_low", ydb.OptionalValue(ydb.Uint64Value(s.TraceIDLow))),
		ydb.StructFieldValue("trace_id_high", ydb.OptionalValue(ydb.Uint64Value(s.TraceIDHigh))),
		ydb.StructFieldValue("span_id", ydb.OptionalValue(ydb.Uint64Value(s.SpanID))),
		ydb.StructFieldValue("operation_name", ydb.OptionalValue(ydb.UTF8Value(s.OperationName))),
		ydb.StructFieldValue("flags", ydb.OptionalValue(ydb.Uint32Value(s.Flags))),
		ydb.StructFieldValue("start_time", ydb.OptionalValue(ydb.Int64Value(s.StartTime))),
		ydb.StructFieldValue("duration", ydb.OptionalValue(ydb.Int64Value(s.Duration))),
		ydb.StructFieldValue("extra", ydb.OptionalValue(ydb.StringValue(s.Extra))),
	)
	return val
}
