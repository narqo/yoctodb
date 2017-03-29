package yoctodb

import (
	"context"
	"fmt"
)

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

func (db *DB) DocumentsCount() int {
	return db.payload.Size()
}

func (db *DB) Query(ctx context.Context, q Query) (*Documents, error) {
	bs, err := q.filteredUnlimited(db)
	if err != nil {
		return nil, err
	}
	if bs == nil {
		return nil, nil
	}

	docs := &Documents{
		db: db,
		bs: bs,
		currentDoc: bs.NextSet(0),
	}
	return docs, nil
}

func (db *DB) Count(ctx context.Context, q Query) (int, error) {
	bs, err := q.filteredUnlimited(db)
	if err != nil {
		return 0, err
	}
	if bs == nil {
		return 0, nil
	}
	defer ReleaseBitSet(bs)

	offset, err := q.offset()
	if err != nil {
		return 0, err
	}
	limit, err := q.limit()
	if err != nil {
		return 0, err
	}
	count := uint(bs.Cardinality()) - offset
	if count < 0 {
		count = 0
	}
	if limit > 0 && count > limit {
		return int(limit), nil
	}
	return int(count), nil
}

type Documents struct {
	db *DB
	bs BitSet

	currentDoc int
}

func (d *Documents) Next() (ok bool) {
	return d.currentDoc > 0
}

func (d *Documents) Scan() error {
	id := d.currentDoc
	d.currentDoc = d.bs.NextSet(d.currentDoc + 1)
	fmt.Printf("scan: id %d\n", id)
	return nil
}

func (d *Documents) Close() {
	ReleaseBitSet(d.bs)
}

func (d *Documents) Err() error {
	return nil
}
