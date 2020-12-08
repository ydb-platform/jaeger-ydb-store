package watcher

import (
	lru "github.com/hashicorp/golang-lru"
)

func mustNewLRU(size int) *lru.Cache {
	cache, err := lru.New(size)
	if err != nil {
		panic(err)
	}
	return cache
}
