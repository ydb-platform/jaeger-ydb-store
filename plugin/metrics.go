package plugin

import (
	"github.com/uber/jaeger-lib/metrics"
	"github.com/yandex-cloud/ydb-go-sdk/table"
	"sync"
)

func tableClientMetrics(factory metrics.Factory) table.ClientTrace {
	m := map[string]struct{}{}
	ns := factory.Namespace(metrics.NSOptions{Name: "tc"})
	sc := ns.Gauge(metrics.Options{Name: "sessions"})
	mx := new(sync.Mutex)
	return table.ClientTrace{
		CreateSessionDone: func(info table.CreateSessionDoneInfo) {
			mx.Lock()
			defer mx.Unlock()
			if info.Error == nil {
				m[info.Session.ID] = struct{}{}
				sc.Update(int64(len(m)))
			}
		},
		DeleteSessionDone: func(info table.DeleteSessionDoneInfo) {
			mx.Lock()
			defer mx.Unlock()
			delete(m, info.Session.ID)
			sc.Update(int64(len(m)))
		},
	}
}
