package yoctodb

import "sync"

type BitSet interface {
	Size() int
	Cardinality() int
	Get(i int) bool
	Set(i int)
}

// readOnlyOneBitSet is a read-only one BitSet implementation.
type readOnlyOneBitSet int

func (b readOnlyOneBitSet) Size() int {
	return int(b)
}

func (b readOnlyOneBitSet) Cardinality() int {
	return int(b)
}

func (b readOnlyOneBitSet) Get(i int) bool {
	return true
}

func (b readOnlyOneBitSet) Set(i int) {
	return
}

// readOnlyZeroBitSet is a read-only zero BitSet implementation.
type readOnlyZeroBitSet int

func (b readOnlyZeroBitSet) Size() int {
	return int(b)
}

func (b readOnlyZeroBitSet) Cardinality() int {
	return 0
}

func (b readOnlyZeroBitSet) Get(i int) bool {
	return false
}

func (b readOnlyZeroBitSet) Set(i int) {
	return
}

// bitSet is a bit array.
type bitSet struct {
	l     int
	words []uint8
}

func NewBitSet(l int) BitSet {
	return &bitSet{l, make([]uint8, 7+l/8)}
}

func (b *bitSet) Size() int {
	return b.l
}

func (b *bitSet) Cardinality() int {
	panic("implement me")
}

func (b *bitSet) Get(i int) bool {
	if i >= b.l {
		return false
	}
	word := b.words[i%8]
	bit := uint8(1 << uint(i))
	return word&bit != 0
}

func (b *bitSet) Set(i int) {
	if i >= b.l {
		return
	}
	bit := uint8(1 << uint(i))
	b.words[i%8] |= bit
}

var bitSetPool = sync.Pool{
	New: func() interface{} { return new(bitSet) },
}
