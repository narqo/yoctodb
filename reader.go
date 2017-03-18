package yoctodb

import (
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
)

var dbFormatMagic = []byte{0x40, 0xC7, 0x0D, 0xB1}

const (
	DBFormatVersion    = 5
	DBFormatDigestSize = md5.Size
)

const (
	SegmentFixedLengthFilter        uint32 = 1000 * (iota + 1)
	segmentVarLenFilter
	SegmentFixedLengthSortableIndex
	SegmentVarLenSortableIndex
	SegmentFixedLengthFullIndex
	SegmentVarLenFullIndex
)

const (
	MultiMapListBased   uint32 = 1000 * (iota + 1)
	MultiMapBitSetBased
)

var (
	ErrWrongMagic    = errors.New("wrong magic")
	ErrShortData     = errors.New("short data")
	ErrCorruptedData = errors.New("data is corrupted")
)

func ReadDB(data io.Reader) error {
	return readDB(data, false)
}

func ReadVerifyDB(data io.Reader) error {
	return readDB(data, true)
}

func readDB(data io.Reader, verifyChecksum bool) error {
	// check the magic
	rawMagic := make([]byte, len(dbFormatMagic))
	if _, err := data.Read(rawMagic); err != nil {
		return fmt.Errorf("could not read magic: %v", err)
	}
	if !bytes.Equal(dbFormatMagic, rawMagic) {
		return ErrWrongMagic
	}

	// check format version
	var version uint32
	if err := readUint32(data, &version); err != nil {
		return fmt.Errorf("could not read version: %v", err)
	}

	if version != DBFormatVersion {
		return fmt.Errorf("version format %d is not supported", version)
	}
	fmt.Printf("parsed version: %d\n", version)

	// check document count
	// TODO(varankinv): format version 6 adds document count
	/*
	if _, err := data.Read(header[:]); err != nil {
		return fmt.Errorf("wrong document count: %v", err)
	}
	docCount := binary.BigEndian.Uint32(header)
	fmt.Printf("parsed document count: %d\n", docCount)
	*/

	buf, err := ioutil.ReadAll(data)
	if err != nil {
		return fmt.Errorf("count not read remaining data: %v", err)
	}
	if len(buf) < DBFormatDigestSize {
		return ErrShortData
	}

	body, origDigest := buf[:len(buf)-DBFormatDigestSize], buf[len(buf)-DBFormatDigestSize:]
	if verifyChecksum {
		bodyDigest := md5.Sum(body)
		if !bytes.Equal(origDigest, bodyDigest[:]) {
			return ErrCorruptedData
		}
	}

	sr := NewSegmentReader(bytes.NewReader(body))
	for !sr.Empty() {
		_, err := sr.ReadSegment()
		if err != nil {
			return err
		}
	}

	return nil
}

type SegmentReader struct {
	r *bytes.Reader
	/// header contains segment's header (size + type)
	header [12]byte
	// offset contains segment's absolute offset
	offset int64
}

func NewSegmentReader(r *bytes.Reader) *SegmentReader {
	return &SegmentReader{
		r: r,
	}
}

func (s *SegmentReader) Empty() bool {
	return s.r.Len() == 0
}

func (s *SegmentReader) ReadSegment() (v interface{}, err error) {
	if _, err = s.r.Read(s.header[:]); err != nil {
		return
	}
	size := binary.BigEndian.Uint64(s.header[0:])
	typ := binary.BigEndian.Uint32(s.header[8:])

	fmt.Printf("read segment: type %d, size %d\n", typ, size)

	sr := io.LimitReader(s.r, int64(size))
	s.offset += int64(len(s.header)) + int64(size)

	var segment interface{}

	switch typ {
	case segmentVarLenFilter:
		var segmentName []byte
		if err := readBytes(sr, &segmentName); err != nil {
			return nil, err
		}

		var chunkLen uint64

		if err := readUint64(sr, &chunkLen); err != nil {
			return nil, err
		}
		if chunkLen == 0 {
			return nil, errors.New("empty segment")
		}

		vals, err := NewVarLenSortedSet(io.LimitReader(sr, int64(chunkLen)))
		if err != nil {
			return nil, fmt.Errorf("count not read segment vals %d: %v", typ, err)
		}

		if err := readUint64(sr, &chunkLen); err != nil {
			return nil, err
		}
		if chunkLen == 0 {
			return nil, errors.New("empty segment")
		}

		idxr := io.LimitReader(sr, int64(chunkLen))

		var mmtyp uint32
		if err := readUint32(idxr, &mmtyp); err != nil {
			return nil, err
		}
		fmt.Printf("read segment: mmtype %d\n", mmtyp)

		var docs interface{}

		switch mmtyp {
		case MultiMapListBased:
		case MultiMapBitSetBased:
			docs, err = NewBitSetIndexToIndexMultiMap(idxr)
			if err != nil {
				return nil, fmt.Errorf("count not read segment docs %d: %v", typ, err)
			}
		}

		segment = &FilterableIndex{
			Name: string(segmentName),
			vals: vals,
			docs: docs,
		}
	}

	fmt.Printf("parsed segment: %+v\n", segment)

	// skip to next segment
	if _, err := s.r.Seek(s.offset, io.SeekStart); err != nil {
		return nil, err
	}

	return segment, nil
}

// SortedSet represents sorted set of values used for filtering and sorting.
type SortedSet interface {
	Get(i int) ([]byte, error)
}

// IndexToIndexMultiMap stores an inverse mapping from a value index to document indexes.
type IndexToIndexMultiMap interface {
}

// TODO(varankinv): IndexToIndexMap

// TODO(varankinv): ByteArrayIndexedList

// FilterableIndex is a filterable segment for each named filterable field.
type FilterableIndex struct {
	Name string
	vals SortedSet
	docs IndexToIndexMultiMap
}

type VarLenSortedSet struct {
	size    int
	offsets []byte
	elems   []byte
}

func NewVarLenSortedSet(r io.Reader) (*VarLenSortedSet, error) {
	var err error

	var size uint32
	if err := readUint32(r, &size); err != nil {
		return nil, err
	}
	offsetsLen := (size+1)<<3 // e.g. size of int64 elements in "offset" chunk

	data, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}

	res := &VarLenSortedSet{
		size: int(size),
		offsets: data[:offsetsLen],
		elems: data[offsetsLen:],
	}

	return res, nil
}

func (v *VarLenSortedSet) Get(i int) ([]byte, error) {
	if i < 0 || i >= v.size {
		return nil, errors.New("out of range")
	}
	base := i << 3
	ofr := bytes.NewReader(v.offsets[base:])

	var start, end uint64

	if err := readUint64(ofr, &start); err != nil {
		return nil, err
	}
	if err := readUint64(ofr, &end); err != nil {
		return nil, err
	}

	if start > end {
		return nil, errors.New("out of range")
	}

	size := end - start
	buf := make([]byte, size)
	copy(buf, v.elems[start:end-start])

	return buf, nil
}

type BitSetIndexToIndexMultiMap struct {
	keysCount int
	size      int
	elems     []byte
}

func NewBitSetIndexToIndexMultiMap(r io.Reader) (*BitSetIndexToIndexMultiMap, error) {
	var n uint32

	if err := readUint32(r, &n); err != nil {
		return nil, err
	}
	res := &BitSetIndexToIndexMultiMap{
		keysCount: int(n),
	}

	if err := readUint32(r, &n); err != nil {
		return nil, err
	}
	res.size = int(n)

	var err error
	if res.elems, err = ioutil.ReadAll(r); err != nil {
		return nil, err
	}

	return res, nil
}

func readUint32(r io.Reader, v *uint32) error {
	b := make([]byte, 4)
	if _, err := r.Read(b); err != nil {
		return err
	}
	*v = binary.BigEndian.Uint32(b)
	return nil
}

func readUint64(r io.Reader, v *uint64) error {
	b := make([]byte, 8)
	if _, err := r.Read(b); err != nil {
		return err
	}
	*v = binary.BigEndian.Uint64(b)
	return nil
}

func readBytes(r io.Reader, v *[]byte) error {
	var n uint32
	if err := readUint32(r, &n); err != nil {
		return err
	}

	*v = make([]byte, n)
	_, err := io.ReadFull(r, *v)

	return err
}
