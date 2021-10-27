package schema

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestPartitionKey_TimeSpan(t *testing.T) {
	ts := time.Date(2063, 4, 5, 1, 0, 0, 0, time.UTC)
	b := PartitionFromTime(ts)
	bStart, bEnd := b.TimeSpan()
	assert.NotEqual(t, bEnd, bStart)
	diff := bEnd.Sub(bStart)
	assert.Equal(t, partitionStep, diff)
}

func TestMakePartitionList(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		ts := time.Now()
		result := MakePartitionList(ts, ts.Add(time.Nanosecond))
		assert.Len(t, result, 1)
	})
	t.Run("sametime", func(t *testing.T) {
		ts := time.Now()
		result := MakePartitionList(ts, ts)
		assert.Len(t, result, 1)
	})
	t.Run("edge", func(t *testing.T) {
		ts := time.Date(2063, 4, 5, 0, 0, 0, 0, time.UTC)
		result := MakePartitionList(ts, ts.Add(partitionStep))
		assert.Len(t, result, 2)
		assert.Equal(t, result[0].Date, "20630405")
		assert.Equal(t, result[0].Num, uint8(0))
	})
	t.Run("daily", func(t *testing.T) {
		ts := time.Now().Truncate(time.Hour * 24)
		result := MakePartitionList(ts, ts.Add(time.Hour*23))
		assert.Len(t, result, int(numPartitions))
		for i := uint8(0); i < uint8(numPartitions); i++ {
			assert.Equal(t, i, result[i].Num)
		}
	})
}
