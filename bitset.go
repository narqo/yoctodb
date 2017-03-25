package yoctodb

import (
	"errors"
	"fmt"
	"sync"
)

type BitSet interface {
	// Size returns number of bits in BitSet.
	Size() int
	// Cardinality calculates number of ones.
	Cardinality() int
	// Test checks if i-th bit is set.
	Test(i int) bool
	// Set sets i-th bit.
	Set(i int)
	// Reset resets BitSet.
	Reset()
	And(b1 BitSet) (bool, error)
	Or(b1 BitSet) (bool, error)
}

// readOnlyOneBitSet is a read-only one BitSet implementation.
type readOnlyOneBitSet int

var _ BitSet = readOnlyOneBitSet(5)

func (b readOnlyOneBitSet) Size() int {
	return int(b)
}

func (b readOnlyOneBitSet) Cardinality() int {
	return int(b)
}

func (b readOnlyOneBitSet) Test(i int) bool {
	return true
}

func (b readOnlyOneBitSet) Set(i int) {
	return
}

func (b readOnlyOneBitSet) Reset() {
	return
}

func (b readOnlyOneBitSet) And(b1 BitSet) (bool, error) {
	return false, errors.New("read-only BitSet")
}

func (b readOnlyOneBitSet) Or(b1 BitSet) (bool, error) {
	return false, errors.New("read-only BitSet")
}

// readOnlyZeroBitSet is a read-only zero BitSet implementation.
type readOnlyZeroBitSet int

func (b readOnlyZeroBitSet) Size() int {
	return int(b)
}

func (b readOnlyZeroBitSet) Cardinality() int {
	return 0
}

func (b readOnlyZeroBitSet) Test(i int) bool {
	return false
}

func (b readOnlyZeroBitSet) Set(i int) {
	return
}

func (b readOnlyZeroBitSet) Reset() {
	return
}

func (b readOnlyZeroBitSet) And(b1 BitSet) (bool, error) {
	return false, errors.New("read-only BitSet")
}

func (b readOnlyZeroBitSet) Or(b1 BitSet) (bool, error) {
	return false, errors.New("read-only BitSet")
}

func bitSetWordSize(n uint) uint {
	return uint(n)>>6 + 1
}

const wordOfOnes = 1<<64 - 1

// bitSet is a bit array.
type bitSet struct {
	size  int
	words []uint64
}

var _ BitSet = &bitSet{}

func NewBitSet(size int) BitSet {
	wordSize := bitSetWordSize(uint(size))
	return &bitSet{size, make([]uint64, wordSize)}
}

func NewBitSetOfOnes(size int) BitSet {
	wordSize := bitSetWordSize(uint(size))
	b := &bitSet{size, make([]uint64, wordSize)}
	for i := 0; i < len(b.words)-1; i++ {
		b.words[i] = wordOfOnes
	}
	lastWordBit := uint(size) & 63 // size mod 64
	if lastWordBit != 0 {
		b.words[len(b.words)-1] = ^(wordOfOnes << lastWordBit)
	}
	return b
}

func (b *bitSet) Size() int {
	return b.size
}

func (b *bitSet) Cardinality() (n int) {
	wordSize := bitSetWordSize(uint(b.size))
	for i := uint(0); i < wordSize; i++ {
		n += int(popcount(b.words[i]))
	}
	return
}

func (b *bitSet) Test(i int) bool {
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

func (b *bitSet) Reset() {
	for i := 0; i < len(b.words); i++ {
		b.words[i] = 0
	}
}

func (b *bitSet) Inverse() {
	panic("implement me")
}

func (b *bitSet) And(b1 BitSet) (bool, error) {
	if b.Size() != b1.Size() {
		return false, fmt.Errorf("BitSets of not equal sizes: %d, %d", b.Size(), b1.Size())
	}

	var words []uint64
	switch b1 := b1.(type) {
	case *bitSet:
		words = b1.words
	default:
		panic("implement me")
	}

	var notEmpty bool
	wordSize := bitSetWordSize(uint(b.size))

	for i := uint(0); i < wordSize; i++ {
		b.words[i] &= words[i]
		if b.words[i] != 0 {
			notEmpty = true
		}
	}

	return notEmpty, nil
}

func (b *bitSet) Or(b1 BitSet) (bool, error) {
	if b.Size() != b1.Size() {
		return false, fmt.Errorf("BitSets of not equal sizes: %d, %d", b.Size(), b1.Size())
	}

	var words []uint64
	switch b1 := b1.(type) {
	case *bitSet:
		words = b1.words
	default:
		panic("implement me")
	}

	var notEmpty bool
	wordSize := bitSetWordSize(uint(b.size))

	for i := uint(0); i < wordSize; i++ {
		b.words[i] |= words[i]
		if b.words[i] != 0 {
			notEmpty = true
		}
	}

	return notEmpty, nil
}

var bitSetPool = sync.Pool{
	New: func() interface{} { return new(bitSet) },
}

// popcount calculates bit population count (aka bitCount).
// Credits to https://github.com/willf/bitset/pull/21
func popcount(n uint64) uint64 {
	n -= (n >> 1) & 0x5555555555555555
	n = (n>>2)&0x3333333333333333 + n&0x3333333333333333
	n += n >> 4
	n &= 0x0f0f0f0f0f0f0f0f
	n *= 0x0101010101010101
	return n >> 56
}
