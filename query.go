package yoctodb

import "errors"

type Query interface {
	// filteredUnlimited calculates filtering result not taking into account skip/limit.
	filteredUnlimited(db *DB) (BitSet, error)
}

type Select struct {
	Where   Condition
	OrderBy interface{} // TODO(varankinv): type orderBy
	Limit   uint32
}

var _ Query = &Select{}

func (s *Select) filteredUnlimited(db *DB) (BitSet, error) {
	if s.Where == nil {
		bs := readOnlyOneBitSet(db.DocumentCount())
		return bs, nil
	}
	// TODO(varankinv): acquire bitSet
	bs := NewBitSet(db.DocumentCount())
	ok, err := s.Where.Set(db, bs)
	if err != nil {
		return nil, err
	}
	if !ok {
		// release bitSet
		return nil, nil
	}
	return bs, nil
}

type Condition interface {
	// Set sets bits for the documents satisfying condition and leave all the other bits untouched.
	Set(db *DB, v BitSet) (bool, error)
}

func Eq(name string, val []byte) Condition {
	return &eqCondition{name, val}
}

type eqCondition struct {
	Name string
	Value []byte
}

func (c *eqCondition) Set(db *DB, v BitSet) (bool, error) {
	index := db.Filter(c.Name)
	if index == nil {
		return false, nil
	}
	return index.Eq(c.Value, v)
}

type And []Condition

func (c *And) Set(db *DB, v BitSet) (bool, error) {
	if len(*c) == 0 {
		return false, errors.New("no conditions")
	}
	if len(*c) == 1 {
		return c.setOne(0, db, v)
	}

	// TODO(varankinv): acquire BitSet
	res := NewBitSet(v.Size())
	ok, err := c.setOne(1, db, res)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}

	tres := NewBitSet(v.Size())
	for n := 2; n < len(*c); n++ {
		tres.Reset()

		ok, err := c.setOne(n, db, tres)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
		anyBitSet, err := res.And(tres)
		if err != nil {
			return false, err
		}
		if !anyBitSet {
			return false, nil
		}
	}
	return v.Or(res)
}

func (c *And) setOne(n int, db *DB, v BitSet) (bool, error) {
	return (*c)[n].Set(db, v)
}
