package dbmodel

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/jaegertracing/jaeger/model"
)

const (
	NumIndexBuckets = 10
)

var (
	errScanTraceID = errors.New("failed to scan TraceID")
	errListLength  = errors.New("invalid length for TraceIDList")
)

// TraceID represents db-serializable trace id
type TraceID [16]byte

func TraceIDFromDomain(src model.TraceID) TraceID {
	res := TraceID{}
	binary.BigEndian.PutUint64(res[:8], src.High)
	binary.BigEndian.PutUint64(res[8:], src.Low)
	return res
}

// Scan converts db result bytes slice to TraceID type
func (dbTraceID *TraceID) Scan(src interface{}) error {
	switch v := src.(type) {
	case []byte:
		copy(dbTraceID[:], v[:16])
		return nil
	default:
		return errScanTraceID
	}
}

// ToDomain converts trace ID from db-serializable form to domain TradeID
func (dbTraceID TraceID) ToDomain() model.TraceID {
	traceIDHigh := binary.BigEndian.Uint64(dbTraceID[:8])
	traceIDLow := binary.BigEndian.Uint64(dbTraceID[8:])
	return model.NewTraceID(traceIDHigh, traceIDLow)
}

type TraceIDList []TraceID

func (t *TraceIDList) Scan(src interface{}) error {
	var in []byte
	switch v := src.(type) {
	case []byte:
		in = v
	case string:
		in = []byte(v)
	default:
		return fmt.Errorf("invalid trace id list type: %T", src)
	}

	lst, err := TraceIDListFromBytes(in)
	if err != nil {
		return err
	}
	*t = lst
	return nil
}

func TraceIDListFromBytes(buf []byte) (TraceIDList, error) {
	if len(buf)%16 != 0 {
		return nil, errListLength
	}
	n := len(buf) / 16
	l := make(TraceIDList, n)
	tid := TraceID{}
	for i := 0; i < n; i++ {
		if err := tid.Scan(buf[i*16:]); err != nil {
			return nil, err
		}
		l[i] = tid
	}
	return l, nil
}

type IndexResult struct {
	Ids   TraceIDList
	RevTs int64
}
