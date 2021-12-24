package dbmodel

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

func (s *Span) StructValue() types.Value {
	return types.StructValue(
		types.StructFieldValue("trace_id_low", types.OptionalValue(types.Uint64Value(s.TraceIDLow))),
		types.StructFieldValue("trace_id_high", types.OptionalValue(types.Uint64Value(s.TraceIDHigh))),
		types.StructFieldValue("span_id", types.OptionalValue(types.Uint64Value(s.SpanID))),
		types.StructFieldValue("operation_name", types.OptionalValue(types.UTF8Value(s.OperationName))),
		types.StructFieldValue("flags", types.OptionalValue(types.Uint32Value(s.Flags))),
		types.StructFieldValue("start_time", types.OptionalValue(types.Int64Value(s.StartTime))),
		types.StructFieldValue("duration", types.OptionalValue(types.Int64Value(s.Duration))),
		types.StructFieldValue("extra", types.OptionalValue(types.StringValue(s.Extra))),
	)
}
