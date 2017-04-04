package yoctodb

import (
	"errors"
)

type Query interface {
	// filteredUnlimited calculates filtering result.
	filteredUnlimited(db *DB) (BitSet, error)
	exec(db *DB) (*Documents, error)
	limit() (uint, error)
	offset() (uint, error)
}

type Select struct {
	Where   Condition
	OrderBy newScorerFunc
	Limit   uint32
	Offset  uint32
}

var _ Query = &Select{}

func (s *Select) filteredUnlimited(db *DB) (BitSet, error) {
	if s.Where == nil {
		bs := readOnlyOneBitSet(db.DocumentsCount())
		return bs, nil
	}
	bs := AcquireBitSet(db.DocumentsCount())
	ok, err := s.Where.Set(db, bs)
	if err != nil {
		ReleaseBitSet(bs)
		return nil, err
	}
	if !ok {
		ReleaseBitSet(bs)
		return nil, nil
	}
	return bs, nil
}

func (s *Select) exec(db *DB) (*Documents, error) {
	bs, err := s.filteredUnlimited(db)
	if err != nil {
		return nil, err
	}
	if bs == nil {
		bs = readOnlyZeroBitSet(db.DocumentsCount())
	}

	offset, err := s.offset()
	if err != nil {
		return nil, err
	}

	var scorer Scorer
	if s.OrderBy != nil {
		scorer = s.OrderBy(db, bs)
	} else {
		scorer = &idScorer{
			db: db,
			bs: bs,
		}
	}

	docs := &Documents{
		db:         db,
		scorer:     scorer,
		skip:       int(offset),
		currentDoc: -1,
	}
	return docs, nil
}

func (s *Select) limit() (uint, error) {
	return uint(s.Limit), nil
}

func (s *Select) offset() (uint, error) {
	return uint(s.Offset), nil
}

type Condition interface {
	// Set sets bits for the Documents satisfying condition and leave all the other bits untouched.
	Set(db *DB, v BitSet) (bool, error)
}

func Eq(name string, val []byte) Condition {
	return &eqCondition{name, val}
}

type eqCondition struct {
	Name  string
	Value []byte
}

func (c *eqCondition) Set(db *DB, v BitSet) (bool, error) {
	index := db.Filter(c.Name)
	if index == nil {
		return false, nil
	}
	return index.Eq(c.Value, v)
}

func And(conditions ...Condition) Condition {
	c := andCondition(conditions)
	return &c
}

type andCondition []Condition

func (c *andCondition) Set(db *DB, v BitSet) (bool, error) {
	if len(*c) == 0 {
		return false, errors.New("no conditions")
	}
	if len(*c) == 1 {
		return c.setOne(0, db, v)
	}

	res := AcquireBitSet(v.Size())
	defer ReleaseBitSet(res)

	ok, err := c.setOne(0, db, res)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}

	claRes := AcquireBitSet(v.Size())
	defer ReleaseBitSet(claRes)

	for n := 1; n < len(*c); n++ {
		claRes.Reset()

		ok, err := c.setOne(n, db, claRes)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
		anyBitSet, err := res.And(claRes)
		if err != nil {
			return false, err
		}
		if !anyBitSet {
			return false, nil
		}
	}

	return v.Or(res)
}

func (c *andCondition) setOne(n int, db *DB, v BitSet) (bool, error) {
	return (*c)[n].Set(db, v)
}

func Or(conditions ...Condition) Condition {
	c := orCondition(conditions)
	return &c
}

type orCondition []Condition

func (c *orCondition) Set(db *DB, v BitSet) (res bool, err error) {
	for n := 0; n < len(*c); n++ {
		anyBitSet, err := (*c)[n].Set(db, v)
		if err != nil {
			return false, err
		}
		if anyBitSet {
			res = true
		}
	}
	return
}

// Scorer is an iterator over documents matching query.
type Scorer interface {
	next(n int) (int, bool)
	close() error
}

type idScorer struct {
	db *DB
	bs BitSet
}

func (s *idScorer) next(n int) (int, bool) {
	n = s.bs.NextSet(n)
	ok := n >= 0
	if !ok {
		s.close()
	}
	return n, ok
}

func (s *idScorer) close() error {
	if s.bs != nil {
		ReleaseBitSet(s.bs)
		s.bs = nil
	}
	return nil
}

type sortingScorer struct {
	db      *DB
	bs      BitSet
	sorts   []string
	indexes []*SortableIndex
}

func (s *sortingScorer) next(n int) (int, bool) {
	panic("implement me")
}

func (s *sortingScorer) close() error {
	panic("implement me")
}

type newScorerFunc func(db *DB, bs BitSet) Scorer

func newSortingScorer(db *DB, bs BitSet, order int, sorts ...string) Scorer {
	scorer := &sortingScorer{
		db: db,
		bs: bs,
		sorts: sorts,
		indexes: make([]*SortableIndex, len(sorts)),
	}

	for i, name := range sorts {
		scorer.indexes[i] = db.Sorter(name)
	}

	return scorer
}

func Asc(sorts ...string) newScorerFunc {
	return newScorerFunc(func(db *DB, bs bitSet) Scorer {
		return newSortingScorer(db, bs, 1, sorts...)
	})
}

func Desc(sorts ...string) newScorerFunc {
	return newScorerFunc(func(db *DB, bs bitSet) Scorer {
		return newSortingScorer(db, bs, -1, sorts...)
	})
}

// Documents is an iterable collection of query execution results.
type Documents struct {
	db     *DB
	scorer Scorer

	closed     bool
	skip       int
	currentDoc int
}

func (d *Documents) Next() (ok bool) {
	if d.closed {
		return false
	}
	d.currentDoc, ok = d.scorer.next(d.currentDoc + 1)
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
	if err := d.scorer.close(); err != nil {
		return err
	}
	d.closed = true
	return nil
}

func (d *Documents) Err() error {
	// TODO(varankinv): Documents.Err()
	return nil
}

type DocumentProcessor interface {
	Process(d int, rawData []byte) error
}
