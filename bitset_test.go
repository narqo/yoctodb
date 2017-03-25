package yoctodb

import (
	"testing"
)

func TestBitSet(t *testing.T) {
	lens := []int{3, 5, 8, 11, 143}
	for _, l := range lens {
		b := NewBitSet(l)
		testBitSet(t, "init run", l, b)
		b.Reset()
		testBitSet(t, "after reset", l, b)
	}
}

func testBitSet(t *testing.T, prefix string, l int, b BitSet) {
	if b.Size() != l {
		t.Fatalf("(%s) Size() want %d, got %d", prefix, b.Size(), l)
	}
	for i := 0; i < l; i++ {
		if b.Get(i) {
			t.Fatalf("(%s) expect bit %d to be unset", prefix, i)
		}
		b.Set(i)
		if !b.Get(i) {
			t.Fatalf("(%s) expect bit %d to be set", prefix, i)
		}
	}
}
