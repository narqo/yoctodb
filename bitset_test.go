package yoctodb

import (
	"testing"
)

func TestBitSet(t *testing.T) {
	lens := []int{3, 5, 8, 11, 143}
	for _, l := range lens {
		b := NewBitSet(l)
		testBitSet(t, l, b)
	}
}

func testBitSet(t *testing.T, l int, b BitSet) {
	if b.Size() != l {
		t.Fatalf("Size() want %d, got %d", b.Size(), l)
	}
	for i := 0; i < l; i++ {
		if b.Get(i) {
			t.Fatalf("expect bit %d to be unset", i)
		}
		b.Set(i)
		if !b.Get(i) {
			t.Fatalf("expect bit %d to be set", i)
		}
	}
}
