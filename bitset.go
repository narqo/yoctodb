package yoctodb

import "sync"

type BitSet interface {
	Size() uint32
	Cardinality() uint32
	Get(i uint32) bool
	Set(i uint32)
}

// readOnlyOneBitSet is a read-only one BitSet implementation.
type readOnlyOneBitSet uint32

func (b readOnlyOneBitSet) Size() uint32 {
	return uint32(b)
}

func (b readOnlyOneBitSet) Cardinality() uint32 {
	return uint32(b)
}

func (b readOnlyOneBitSet) Get(i uint32) bool {
	return true
}

func (b readOnlyOneBitSet) Set(i uint32) {
	return
}

// readOnlyZeroBitSet is a read-only zero BitSet implementation.
type readOnlyZeroBitSet uint32

func (b readOnlyZeroBitSet) Size() uint32 {
	return uint32(b)
}

func (b readOnlyZeroBitSet) Cardinality() uint32 {
	return 0
}

func (b readOnlyZeroBitSet) Get(i uint32) bool {
	return false
}

func (b readOnlyZeroBitSet) Set(i uint32) {
	return
}

// bitSet...
type bitSet []byte

func (b *bitSet) Size() uint32 {
	return uint32(len(*b))
}

func (b *bitSet) Cardinality() uint32 {
	panic("implement me")
}

func (b *bitSet) Get(i uint32) bool {
	panic("implement me")
}

func (b *bitSet) Set(i uint32) {
	panic("implement me")
}

var bitSetPool = sync.Pool{
	New: func() interface{} { return new(bitSet) },
}
