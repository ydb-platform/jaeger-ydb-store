package indexer

import (
	"github.com/YandexClassifieds/jaeger-ydb-store/storage/spanstore/indexer/index"
	"github.com/jaegertracing/jaeger/model"
	"sync"
	"time"
)

type indexTTLMap struct {
	maxItemsPerKey int
	maxTTL         time.Duration

	evict indexMapEvictFunc
	m     map[indexMapKey]*ttlMapValue
	l     sync.Mutex
}

type indexMapKey struct {
	hash uint64
	ts   int64
}

type ttlMapValue struct {
	idx        index.Indexable
	traceIds   []model.TraceID
	lastAccess time.Time
}

type indexMapEvictFunc func(idx index.Indexable, traceIds []model.TraceID)

func newIndexMap(evict indexMapEvictFunc, maxItemsPerKey int, maxTTL time.Duration) *indexTTLMap {
	if maxItemsPerKey <= 0 {
		panic("maxItemsPerKey invalid value")
	}
	if maxTTL <= 0 {
		panic("maxTTLSeconds invalid value")
	}
	m := &indexTTLMap{
		maxItemsPerKey: maxItemsPerKey,
		maxTTL:         maxTTL,
		evict:          evict,
		m:              make(map[indexMapKey]*ttlMapValue),
	}
	go m.evictProcess()
	return m
}

func (m *indexTTLMap) evictProcess() {
	for now := range time.Tick(time.Second) {
		m.l.Lock()
		for k, v := range m.m {
			if now.Sub(v.lastAccess) >= m.maxTTL {
				delete(m.m, k)
				m.evict(v.idx, v.traceIds)
			}
		}
		m.l.Unlock()
	}
}

func (m *indexTTLMap) Add(idx index.Indexable, traceId model.TraceID) {
	m.l.Lock()
	defer m.l.Unlock()
	key := indexMapKey{
		hash: idx.Hash(),
		ts:   idx.Timestamp().Truncate(time.Second * 5).Unix(),
	}
	var v *ttlMapValue
	if vv, ok := m.m[key]; !ok {
		v = &ttlMapValue{
			idx:      idx,
			traceIds: make([]model.TraceID, 0, m.maxItemsPerKey),
		}
		m.m[key] = v
	} else {
		v = vv
	}
	v.traceIds = append(m.m[key].traceIds, traceId)
	v.lastAccess = time.Now()
	if len(v.traceIds) >= m.maxItemsPerKey {
		delete(m.m, key)
		m.evict(v.idx, v.traceIds)
	}
}
