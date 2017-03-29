package yoctodb

import (
	"errors"
)

type Query interface {
	// filteredUnlimited calculates filtering result not taking into account skip and limit.
	filteredUnlimited(db *DB) (BitSet, error)
	// sortedUnlimited returns sorted results not taking into account skip and limit.
	sortedUnlimited(db *DB, v BitSet) error
	limit() (uint, error)
	offset() (uint, error)
}

type Select struct {
	Where   Condition
	OrderBy interface{} // TODO(varankinv): type orderBy
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

func (s *Select) sortedUnlimited(db *DB, v BitSet) error {
	if s.OrderBy != nil {
		panic("implement me")
	}
	return nil
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
