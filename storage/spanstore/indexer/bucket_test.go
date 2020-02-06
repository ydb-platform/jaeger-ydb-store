package indexer

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBucket(t *testing.T) {
	b := bucketRR{max: 5}
	res := make([]uint8, 0)
	for i := 0; i < 11; i++ {
		res = append(res, b.Next())
	}
	exp := []uint8{0, 1, 2, 3, 4, 0, 1, 2, 3, 4, 0}
	assert.Equal(t, exp, res)
}
