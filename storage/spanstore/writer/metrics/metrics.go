package metrics

import (
	"github.com/uber/jaeger-lib/metrics"
	"time"
)

type WriteMetrics struct {
	Attempts   metrics.Counter `metric:"attempts"`
	Inserts    metrics.Counter `metric:"inserts"`
	Errors     metrics.Counter `metric:"errors"`
	LatencyOk  metrics.Timer   `metric:"latency-ok"`
	LatencyErr metrics.Timer   `metric:"latency-err"`
	RecordsOk  metrics.Counter `metric:"records-ok"`
	RecordsErr metrics.Counter `metric:"records-err"`
}

func NewWriteMetrics(factory metrics.Factory, tableName string) *WriteMetrics {
	t := &WriteMetrics{}
	metrics.Init(t, factory.Namespace(metrics.NSOptions{Name: tableName, Tags: nil}), nil)
	return t
}

func (t *WriteMetrics) Emit(err error, latency time.Duration, count int) {
	t.Attempts.Inc(1)
	if err != nil {
		t.LatencyErr.Record(latency)
		t.Errors.Inc(1)
		t.RecordsErr.Inc(int64(count))
	} else {
		t.LatencyOk.Record(latency)
		t.Inserts.Inc(1)
		t.RecordsOk.Inc(int64(count))
	}
}
