package plugin

import (
	"sync"

	"github.com/uber/jaeger-lib/metrics"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func tableClientMetrics(factory metrics.Factory) trace.Table {
	m := map[string]struct{}{}
	ns := factory.Namespace(metrics.NSOptions{Name: "tc"})
	sc := ns.Gauge(metrics.Options{Name: "sessions"})
	mx := new(sync.Mutex)
	return trace.Table{
		OnSessionNew: func(trace.SessionNewStartInfo) func(trace.SessionNewDoneInfo) {
			return func(doneInfo trace.SessionNewDoneInfo) {
				mx.Lock()
				defer mx.Unlock()
				if doneInfo.Error == nil {
					m[doneInfo.Session.ID()] = struct{}{}
					sc.Update(int64(len(m)))
				}
			}
		},
		OnSessionDelete: func(info trace.SessionDeleteStartInfo) func(trace.SessionDeleteDoneInfo) {
			return func(_ trace.SessionDeleteDoneInfo) {
				mx.Lock()
				defer mx.Unlock()
				delete(m, info.Session.ID())
				sc.Update(int64(len(m)))
			}
		},
	}
}
