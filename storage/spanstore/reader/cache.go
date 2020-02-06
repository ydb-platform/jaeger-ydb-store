package reader

import (
	"sync"
	"time"
)

type ttlCache struct {
	mx sync.RWMutex
	m  map[interface{}]cacheValue
}

type cacheValue struct {
	value   interface{}
	expires int64
}

func newTtlCache() *ttlCache {
	c := &ttlCache{
		m: make(map[interface{}]cacheValue),
	}
	go c.evictProcess()
	return c
}

func (c *ttlCache) Get(key interface{}) (interface{}, bool) {
	c.mx.RLock()
	defer c.mx.RUnlock()
	if v, ok := c.m[key]; ok {
		return v.value, true
	}
	return nil, false
}

func (c *ttlCache) Set(key, value interface{}, ttl time.Duration) {
	c.mx.Lock()
	defer c.mx.Unlock()
	c.m[key] = cacheValue{
		value:   value,
		expires: time.Now().Add(ttl).Unix(),
	}
}

func (c *ttlCache) evictProcess() {
	for t := range time.Tick(time.Second) {
		c.mx.Lock()
		ts := t.Unix()
		for k, v := range c.m {
			if ts > v.expires {
				delete(c.m, k)
			}
		}
		c.mx.Unlock()
	}
}
