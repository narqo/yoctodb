package yoctodb

type DB struct {
	filters map[string]*FilterableIndex
	//sorters map[string]*SortableIndex
	payload *Payload
}

func (db *DB) Filter(name string) *FilterableIndex {
	return db.filters[name]
}

func (db *DB) Document(i int) ([]byte, error) {
	return db.payload.Get(i)
}

func (db *DB) DocumentCount() int {
	return db.payload.Size()
}

func (db *DB) Count(q Query) (int, error) {
	b, err := q.filteredUnlimited(db)
	if err != nil {
		return 0, err
	}
	if b == nil {
		return 0, nil
	}
	offset, err := q.offset()
	if err != nil {
		return 0, err
	}
	limit, err := q.limit()
	if err != nil {
		return 0, err
	}
	count := uint(b.Cardinality()) - offset
	if count < 0 {
		count = 0
	}
	if limit > 0 && count > limit {
		return int(limit), nil
	}
	return int(count), nil
}
