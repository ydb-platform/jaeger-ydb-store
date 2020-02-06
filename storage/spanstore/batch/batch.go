package batch

type batch struct {
	items []interface{}
}

func newBatch(cnt int) *batch {
	return &batch{items: make([]interface{}, 0, cnt)}
}

func (b *batch) Append(item interface{}) {
	b.items = append(b.items, item)
}

func (b *batch) Len() int {
	return len(b.items)
}
