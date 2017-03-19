package yoctodb

type DB struct {
	Version uint8
	filters map[string]*FilterableIndex
	//sorters map[string]*SortableIndex
	payload *Payload
}

func (db *DB) Document(i int) ([]byte, error) {
	return db.payload.Get(i)
}

func (db *DB) DocumentCount() int {
	return db.payload.Size()
}

//func (db *DB) Count(q Query) (int, error) {
//
//}
