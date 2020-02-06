package indexer

import (
	"math/rand"
	"sync"
)

type lockedSource struct {
	lk  sync.Mutex
	src rand.Source
}

func newLockedRand(seed int64) *rand.Rand {
	return rand.New(&lockedSource{src: rand.NewSource(seed)})
}

func (l *lockedSource) Int63() int64 {
	l.lk.Lock()
	defer l.lk.Unlock()
	return l.src.Int63()
}

func (l *lockedSource) Seed(seed int64) {
	l.lk.Lock()
	defer l.lk.Unlock()
	l.src.Seed(seed)
}
