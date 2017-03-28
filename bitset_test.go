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
	b1, b2 := NewBitSetOfOnes(5), NewBitSet(5)

	b2.Set(2)
	b2.Set(4)

	if _, err := b1.And(b2); err != nil {
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
	if _, err := b1.And(b2); err == nil {
		t.Fatal("And() on BitSets of different sizes expected to fail")
	}

	b1, b2 = NewBitSet(5), NewBitSet(5)
	if isAnySet, _ := b1.And(b2); isAnySet {
		t.Fatal("And() on empty BitSets expected to be empty")
	}

	b1, b2 = NewBitSet(5), NewBitSetOfOnes(5)
	if isAnySet, _ := b1.And(b2); isAnySet {
		t.Fatal("And() with non-empty BitSets expected to be empty")
	}

	b1, b2 = NewBitSetOfOnes(5), NewBitSetOfOnes(5)
	if isAnySet, _ := b1.And(b2); !isAnySet {
		t.Fatal("And() of non-empty BitSets expected to not be empty")
	}
}

func TestBitSet_Or(t *testing.T) {
	b1, b2 := NewBitSetOfOnes(5), NewBitSet(5)

	b2.Set(2)
	b2.Set(4)

	if _, err := b1.Or(b2); err != nil {
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
	if _, err := b1.Or(b2); err == nil {
		t.Fatal("Or() on BitSets of different sizes expected to fail")
	}

	b1, b2 = NewBitSet(5), NewBitSet(5)
	if isAnySet, _ := b1.Or(b2); isAnySet {
		t.Fatal("Or() on empty BitSets expected to be empty")
	}

	b1, b2 = NewBitSet(5), NewBitSetOfOnes(5)
	if isAnySet, _ := b1.Or(b2); !isAnySet {
		t.Fatal("Or() with non-empty BitSets expected to not be empty")
	}
}

func TestBitSet_Cardinality(t *testing.T) {
	n := 5
	b := NewBitSet(n)

	if b.Cardinality() != 0 {
		t.Fatal("Cardinality() of empty BitSet expect to be 0")
	}

	for i := 0; i < n; i++ {
		b.Set(i)
		if b.Cardinality() != i + 1 {
			t.Fatalf("(%d) Cardinality() expected to be %d, got %d", i, i, b.Cardinality())
		}
	}
}

func TestBitSet_NextSet(t *testing.T) {
	n := 5
	b := NewBitSet(n)
	b.Set(1)
	b.Set(2)
	b.Set(4)

	tests := []struct{
		In int
		Out int
	}{
		{ 0, 1 },
		{ 1, 1 },
		{ 2, 2 },
		{ 3, 4 },
		{ 4, 4 },
		{ 5, -1 },
	}

	for n, tc := range tests {
		if b.NextSet(tc.In) != tc.Out {
			t.Fatalf("case %d: NextSet(%d), want %d, got %d", n, tc.In, tc.Out, b.NextSet(tc.In))
		}
	}
}
