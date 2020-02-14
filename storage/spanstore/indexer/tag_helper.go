package indexer

import "github.com/jaegertracing/jaeger/model"

var (
	stopList = []string{"sampler.type", "sampler.param", "internal.span.format"}
	stopMap  map[string]struct{}
)

func init() {
	stopMap = make(map[string]struct{}, len(stopList))
	for _, l := range stopList {
		stopMap[l] = struct{}{}
	}
}

func shouldIndexTag(kv model.KeyValue) bool {
	if kv.VType == model.ValueType_BINARY {
		return false
	}
	if _, exists := stopMap[kv.Key]; exists {
		return false
	}
	return true
}
