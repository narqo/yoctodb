package yoctodb

import "fmt"

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
	fmt.Printf("bitset: %064b", b)
	return 0, nil
}
