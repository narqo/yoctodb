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
		if b.Test(i) {
			t.Fatalf("(%s) expect bit %d to be unset", prefix, i)
		}
		b.Set(i)
		if !b.Test(i) {
			t.Fatalf("(%s) expect bit %d to be set", prefix, i)
		}
	}
}

func TestBitSet_And(t *testing.T) {
	l := 5
	b1, b2 := NewBitSetOfOnes(l), NewBitSet(l)

	b2.Set(2)
	b2.Set(4)

	if err := b1.And(b2); err != nil {
		t.Fatal(err)
	}
	if b1.Test(1) {
		t.Error("expect bit 1 to be unset")
	}
	if !b1.Test(2) {
		t.Error("expect bit 2 to be set")
	}
	if !b1.Test(4) {
		t.Error("expect bit 4 to be set")
	}

	b1, b2 = NewBitSet(5), NewBitSet(10)
	if err := b1.And(b2); err == nil {
		t.Fatal("And() on BitSets of different sizes expect to fail")
	}
}

func TestBitSet_Or(t *testing.T) {
	l := 5
	b1, b2 := NewBitSetOfOnes(l), NewBitSet(l)

	b2.Set(2)
	b2.Set(4)

	if err := b1.Or(b2); err != nil {
		t.Fatal(err)
	}
	if !b1.Test(1) {
		t.Error("expect bit 1 to be set")
	}
	if !b1.Test(2) {
		t.Error("expect bit 2 to be set")
	}
	if !b1.Test(4) {
		t.Error("expect bit 4 to be set")
	}

	b1, b2 = NewBitSet(5), NewBitSet(10)
	if err := b1.Or(b2); err == nil {
		t.Fatal("Or() on BitSets of different sizes expect to fail")
	}
}
