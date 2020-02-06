package dbmodel

type UniqueTraceIDs struct {
	m map[TraceID]struct{}
	l []TraceID
}

func NewUniqueTraceIDs() *UniqueTraceIDs {
	return &UniqueTraceIDs{
		m: make(map[TraceID]struct{}),
		l: make([]TraceID, 0),
	}
}

func (m *UniqueTraceIDs) Add(id TraceID) {
	if _, contains := m.m[id]; !contains {
		m.m[id] = struct{}{}
		m.l = append(m.l, id)
	}
}

func (m *UniqueTraceIDs) Has(id TraceID) bool {
	_, ok := m.m[id]
	return ok
}

func (m *UniqueTraceIDs) Len() int {
	return len(m.m)
}

func (m *UniqueTraceIDs) AsList() []TraceID {
	return m.l
}

func (m *UniqueTraceIDs) JoinWith(b *UniqueTraceIDs) {
	for id := range b.m {
		m.Add(id)
	}
}

// IntersectTraceIDs takes a list of UniqueTraceIDs and intersects them.
func IntersectTraceIDs(uniqueTraceIdsList []*UniqueTraceIDs) *UniqueTraceIDs {
	retMe := NewUniqueTraceIDs()
	for key := range uniqueTraceIdsList[0].m {
		keyExistsInAll := true
		for _, otherTraceIds := range uniqueTraceIdsList[1:] {
			if !otherTraceIds.Has(key) {
				keyExistsInAll = false
				break
			}
		}
		if keyExistsInAll {
			retMe.Add(key)
		}
	}
	return retMe
}
