package indexer

import (
	"time"
)

var r = newLockedRand(time.Now().UnixNano())

type bucketRR struct {
	max uint8
	cur uint8
}

func newBucketRR(max uint8) *bucketRR {
	return &bucketRR{
		max: max,
		cur: uint8(r.Intn(int(max))),
	}
}

func (b *bucketRR) Next() uint8 {
	v := b.cur
	b.cur++
	if b.cur == b.max {
		b.cur = 0
	}
	return v
}
