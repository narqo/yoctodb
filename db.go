package yoctodb

import (
	"context"
	"errors"
)

type DB struct {
	filters map[string]*FilterableIndex
	sorters map[string]*SortableIndex
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
		bs = readOnlyZeroBitSet(db.DocumentsCount())
	}

	offset, err := q.offset()
	if err != nil {
		return nil, err
	}

	docs := &Documents{
		db:         db,
		bs:         bs,
		skip:       int(offset),
		currentDoc: -1,
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

// Documents is an iterable collection of query execution results.
type Documents struct {
	db *DB
	bs BitSet

	closed     bool
	skip       int
	currentDoc int
}

func (d *Documents) Next() (ok bool) {
	if d.closed {
		return false
	}
	d.currentDoc = d.bs.NextSet(d.currentDoc + 1)
	ok = d.currentDoc >= 0
	if !ok {
		d.Close()
	}
	return ok
}

func (d *Documents) Scan(p DocumentProcessor) error {
	if d.closed {
		return errors.New("Documents are closes")
	}
	if d.currentDoc == -1 {
		return errors.New("Scan called without Next")
	}

	if d.skip > 0 {
		d.skip -= 1
		return nil
	}

	if p == nil {
		return errors.New("no DocumentProcessor passed")
	}
	rawData, err := d.db.Document(d.currentDoc)
	if err != nil {
		return err
	}
	return p.Process(d.currentDoc, rawData)
}

func (d *Documents) Close() error {
	if d.closed {
		return nil
	}
	d.closed = true
	d.releaseBitSet()
	return nil
}

func (d *Documents) releaseBitSet() {
	ReleaseBitSet(d.bs)
}

func (d *Documents) Err() error {
	// TODO(varankinv): Documents.Err()
	return nil
}

type DocumentProcessor interface {
	Process(d int, rawData []byte) error
}
