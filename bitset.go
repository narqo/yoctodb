package yoctodb

import (
	"sync"
	"fmt"
)

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

func bitSetWordSize(n uint) uint {
	return uint(n) >> 6 + 1
}

const wordOfOnes = ^uint64(0)

// bitSet is a bit array.
type bitSet struct {
	size  int
	words []uint64
}

func NewBitSet(size int) BitSet {
	wordSize := bitSetWordSize(uint(size))
	return &bitSet{size, make([]uint64, wordSize)}
}

func NewBitSetOfOnes(size int) BitSet {
	wordSize := bitSetWordSize(uint(size))
	b := &bitSet{size, make([]uint64, wordSize)}
	for i := 0; i < len(b.words) - 1; i++ {
		b.words[i] = wordOfOnes
	}
	lastWordBit := uint(size) & 63 // size mod 64
	if lastWordBit != 0 {
		b.words[len(b.words) - 1] = ^(wordOfOnes << lastWordBit)
	}
	return b
}

func (b *bitSet) Size() int {
	return b.size
}

func (b *bitSet) Cardinality() int {
	panic("implement me")
}

func (b *bitSet) Get(i int) bool {
	if i >= b.size {
		return false
	}
	word := uint(i) >> 6                // i div 64
	mask := uint64(1 << (uint(i) & 63)) // 1 << (i mod 64)
	return b.words[word]&mask != 0
}

func (b *bitSet) Set(i int) {
	if i >= b.size {
		return
	}
	word := uint(i) >> 6  // i div 64
	bit := uint64(i) & 63 // i mod 64
	b.words[word] |= 1 << bit
}

var bitSetPool = sync.Pool{
	New: func() interface{} { return new(bitSet) },
}
