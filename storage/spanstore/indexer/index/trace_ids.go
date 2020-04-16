package index

import (
	"errors"

	"github.com/jaegertracing/jaeger/model"
)

type TraceIDList []model.TraceID

func (l TraceIDList) ToBytes() []byte {
	buf := make([]byte, 16*len(l))
	var err error
	for i, id := range l {
		_, err = id.MarshalTo(buf[i*16:])
		if err != nil {
			panic(err)
		}
	}
	return buf
}

func TraceIDListFromBytes(b []byte) (TraceIDList, error) {
	if len(b)%16 != 0 {
		return nil, errors.New("trace id unmarshal err: invalid length")
	}
	n := len(b) / 16
	l := make(TraceIDList, n)
	id := model.TraceID{}
	for i := 0; i < n; i++ {
		if err := id.Unmarshal(b[:16]); err != nil {
			return nil, err
		}
		l[i] = id
		b = b[16:]
	}
	return l, nil
}
