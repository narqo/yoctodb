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
	dbFormatDigestSize = md5.Size
)

const (
	// all Documents have payload
	PayloadFull uint32 = 1 + iota

	// no Documents have any payload
	PayloadNone
)

const (
	// filterable segment of fixed length elements
	FixedLenFilterableIndex uint32 = 1000 * (1 + iota)

	// filterable segment of variable length elements
	VarLenFilterableIndex

	// sortable segment of fixed length elements
	FixedLenSortableIndex

	// sortable segment of variable length elements
	VarLenSortableIndex

	// full segment of fixed length elements
	FixedLenFullIndexSegment

	// full segment of variable length elements
	VarLenFullIndexSegment
)

var (
	ErrWrongMagic    = errors.New("wrong magic")
	ErrCorruptedData = errors.New("data is corrupted")
	ErrNoPayload     = errors.New("no payload")
)

func ReadDB(data io.Reader) (*DB, error) {
	return readDB(data, false)
}

func ReadVerifyDB(data io.Reader) (*DB, error) {
	return readDB(data, true)
}

func readDB(data io.Reader, verifyChecksum bool) (*DB, error) {
	// check the magic
	rawMagic := make([]byte, len(dbFormatMagic))
	if _, err := data.Read(rawMagic); err != nil {
		return nil, fmt.Errorf("could not read magic: %v", err)
	}
	if !bytes.Equal(dbFormatMagic, rawMagic) {
		return nil, ErrWrongMagic
	}

	// check format version
	var version uint32
	if err := readUint32(data, &version); err != nil {
		return nil, fmt.Errorf("could not read version: %v", err)
	}

	if version != DBFormatVersion {
		return nil, fmt.Errorf("version format %d is not supported", version)
	}
	//fmt.Printf("parsed version: %d\n", version)

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
		return nil, fmt.Errorf("could not read remaining data: %v", err)
	}
	if len(buf) < dbFormatDigestSize {
		return nil, ErrCorruptedData
	}

	// TODO(varankinv): maybe move to `DB.Verify() error`
	body, origDigest := buf[:len(buf)-dbFormatDigestSize], buf[len(buf)-dbFormatDigestSize:]

	if verifyChecksum {
		bodyDigest := md5.Sum(body)
		if !bytes.Equal(origDigest, bodyDigest[:]) {
			return nil, ErrCorruptedData
		}
	}

	db := &DB{
		filters: make(map[string]*FilterableIndex),
		sorters: make(map[string]*SortableIndex),
	}

	sr := NewSegmentReader(bytes.NewReader(body))
	for !sr.Empty() {
		segment, err := sr.ReadSegment()
		if err != nil {
			return nil, err
		}
		switch s := segment.(type) {
		case *Payload:
			if db.payload != nil {
				return nil, errors.New("duplicate payload")
			}
			db.payload = s

		case *FilterableIndex:
			if _, ok := db.filters[s.Name]; ok {
				return nil, fmt.Errorf("duplicate filterable index for field %q", s.Name)
			}
			db.filters[s.Name] = s

		case *SortableIndex:
			if _, ok := db.sorters[s.Name]; ok {
				return nil, fmt.Errorf("duplicate sortable index for field %q", s.Name)
			}
			db.sorters[s.Name] = s
		}
	}

	if db.payload == nil {
		return nil, ErrNoPayload
	}

	return db, nil
}

const (
	multimapListBased   uint32 = 1000 * (1 + iota)
	multimapBitSetBased
)

type SegmentReader struct {
	r *bytes.Reader
	// header contains segment's header (size + type)
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

	//fmt.Printf("read segment: type %d, size %d\n", typ, size)

	s.offset += int64(len(s.header)) + int64(size)

	var segment interface{}

	sr := io.LimitReader(s.r, int64(size))

	switch typ {
	case PayloadFull:
		segment, err = s.readPayload(sr)
		if err != nil {
			return nil, err
		}

	case PayloadNone:
		var size uint32
		if err := readUint32(sr, &size); err != nil {
			return nil, err
		}
		segment = &EmptyPayload{int(size)}

	case FixedLenFilterableIndex, VarLenFilterableIndex:
		segment, err = s.readFilterable(sr, typ)
		if err != nil {
			return nil, err
		}

	case FixedLenSortableIndex, VarLenSortableIndex:
		segment, err = s.readSortable(sr, typ)
		if err != nil {
			return nil, err
		}
	}

	fmt.Printf("read segment %d: %#v\n", typ, segment)

	// skip to next segment
	if _, err := s.r.Seek(s.offset, io.SeekStart); err != nil {
		return nil, err
	}

	return segment, nil
}

func (s *SegmentReader) readCommonSegmentFields(r io.Reader, typ uint32) (string, SortedSet, IndexToIndexMultiMap, error) {
	var rawName []byte
	if err := readBytes(r, &rawName); err != nil {
		return "", nil, nil, err
	}
	segmentName := string(rawName)

	var chunkLen uint64
	if err := readUint64(r, &chunkLen); err != nil {
		return segmentName, nil, nil, err
	}
	if chunkLen == 0 {
		return segmentName, nil, nil, errors.New("empty segment")
	}

	var (
		vals      SortedSet
		valToDocs IndexToIndexMultiMap
		err       error
	)

	if typ == FixedLenFilterableIndex || typ == FixedLenSortableIndex {
		r := io.LimitReader(r, int64(chunkLen))
		vals, err = NewFixedLenSortedSet(r)
		if err != nil {
			return segmentName, nil, nil, fmt.Errorf("could not read segment values set %d: %v", typ, err)
		}
	} else if typ == VarLenFilterableIndex || typ == VarLenSortableIndex {
		r := io.LimitReader(r, int64(chunkLen))
		vals, err = NewVarLenSortedSet(r)
		if err != nil {
			return segmentName, nil, nil, fmt.Errorf("could not read segment values set %d: %v", typ, err)
		}
	} else {
		return segmentName, nil, nil, fmt.Errorf("unknown filterable segment type: %d", typ)
	}

	if err := readUint64(r, &chunkLen); err != nil {
		return segmentName, nil, nil, err
	}
	if chunkLen == 0 {
		return segmentName, nil, nil, errors.New("empty segment")
	}

	idxr := io.LimitReader(r, int64(chunkLen))

	var mmtyp uint32
	if err := readUint32(idxr, &mmtyp); err != nil {
		return segmentName, nil, nil, err
	}

	switch mmtyp {
	case multimapListBased:
	case multimapBitSetBased:
		valToDocs, err = NewBitSetIndexToIndexMultiMap(idxr)
		if err != nil {
			return segmentName, nil, nil, fmt.Errorf("could not read segment valToDocs %d: %v", typ, err)
		}
	default:
		return segmentName, nil, nil, fmt.Errorf("unknown multimap type %d", mmtyp)
	}

	return segmentName, vals, valToDocs, nil
}

func (s *SegmentReader) readFilterable(r io.Reader, typ uint32) (*FilterableIndex, error) {
	segmentName, vals, valToDocs, err := s.readCommonSegmentFields(r, typ)
	if err != nil {
		return nil, fmt.Errorf("failed to read segment %q: %v", segmentName, err)
	}

	segment := &FilterableIndex{
		Name:      segmentName,
		vals:      vals,
		valToDocs: valToDocs,
	}
	return segment, nil
}

func (s *SegmentReader) readSortable(r io.Reader, typ uint32) (*SortableIndex, error) {
	segmentName, vals, valToDocs, err := s.readCommonSegmentFields(r, typ)
	if err != nil {
		return nil, fmt.Errorf("failed to read segment %q: %v", segmentName, err)
	}

	docToVals, err := NewIntIndexToIndexMap(r)
	if err != nil {
		return nil, fmt.Errorf("failed to read segment %q: could not read segment docToVals %d: %v",
			segmentName, typ, err)
	}

	segment := &SortableIndex{
		Name:      segmentName,
		vals:      vals,
		valToDocs: valToDocs,
		docToVals: docToVals,
	}
	return segment, nil
}

func (s *SegmentReader) readPayload(r io.Reader) (v *Payload, err error) {
	var chunkLen uint64

	if err := readUint64(r, &chunkLen); err != nil {
		return nil, err
	}
	if chunkLen == 0 {
		return nil, errors.New("empty segment")
	}

	r = io.LimitReader(r, int64(chunkLen))
	payload, err := NewVarLenSortedSet(r)
	if err != nil {
		return nil, fmt.Errorf("could not read segment payload %v", err)
	}

	segment := &Payload{
		data: payload,
	}
	return segment, nil
}

// SortedSet represents sorted set of values used for filtering and sorting.
type SortedSet interface {
	Get(i int) ([]byte, error)
	Size() int
	Compare(i int, val []byte) (int, error)
	Index(val []byte) int
}

// IndexToIndexMultiMap stores an inverse mapping from a value index to document indexes.
type IndexToIndexMultiMap interface {
	Get(n int, v BitSet) (bool, error)
}

// IndexToIndexMap stores a direct mapping from a document index to the value index.
type IndexToIndexMap interface {
	Get(n int) (int, error)
}

// TODO(varankinv): ByteArrayIndexedList

// FilterableIndex is a filterable segment for each named filterable field.
type FilterableIndex struct {
	Name      string
	vals      SortedSet
	valToDocs IndexToIndexMultiMap
}

func (f *FilterableIndex) Eq(val []byte, v BitSet) (bool, error) {
	if n := f.vals.Index(val); n != -1 {
		return f.valToDocs.Get(n, v)
	}
	return false, nil
}

// SortableIndex is a sortable segment for each named sortable field.
//
// SortableIndex contains all fields that FilterableIndex do and a persistent
// collection directly mapping a document index to the value index.
type SortableIndex struct {
	Name      string
	vals      SortedSet
	valToDocs IndexToIndexMultiMap
	docToVals IndexToIndexMap
}

// Payload is an import payload segment.
type Payload struct {
	data SortedSet
}

func (p *Payload) Get(i int) ([]byte, error) {
	return p.data.Get(i)
}

func (p *Payload) Size() int {
	return p.data.Size()
}

// EmptyPayload is an immutable payload segment containing only document count.
type EmptyPayload struct {
	Size int
}

var errOutOfBounds = errors.New("out of bounds")

type fixedLenSortedSet struct {
	size     int
	elemSize int
	elems    []byte
}

func NewFixedLenSortedSet(r io.Reader) (*fixedLenSortedSet, error) {
	var err error

	var size, elemSize uint32
	if err := readUint32(r, &size); err != nil {
		return nil, err
	}
	if err := readUint32(r, &elemSize); err != nil {
		return nil, err
	}

	data, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}

	res := &fixedLenSortedSet{
		size:     int(size),
		elemSize: int(elemSize),
		elems:    data,
	}

	return res, nil
}

func (v *fixedLenSortedSet) Get(i int) ([]byte, error) {
	if i < 0 || i >= v.size {
		return nil, errOutOfBounds
	}
	start := i * v.elemSize

	buf := make([]byte, v.elemSize)
	copy(buf, v.elems[start:v.elemSize])

	return buf, nil
}

func (v *fixedLenSortedSet) Compare(i int, val []byte) (int, error) {
	if i < 0 || i >= v.size {
		return 0, errOutOfBounds
	}
	start := i * v.elemSize
	return bytes.Compare(v.elems[start:v.elemSize], val), nil
}

func (v *fixedLenSortedSet) Size() int {
	return v.size
}

func (v *fixedLenSortedSet) Index(val []byte) int {
	return sortedSetIndexByte(v, val)
}

type varLenSortedSet struct {
	size    int
	offsets []byte
	elems   []byte
}

func NewVarLenSortedSet(r io.Reader) (*varLenSortedSet, error) {
	var err error

	var size uint32
	if err := readUint32(r, &size); err != nil {
		return nil, err
	}
	offsetsLen := (size + 1) << 3 // e.g. size of int64 elements in "offset" chunk

	data, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}

	res := &varLenSortedSet{
		size:    int(size),
		offsets: data[:offsetsLen],
		elems:   data[offsetsLen:],
	}

	return res, nil
}

func (v *varLenSortedSet) Get(i int) ([]byte, error) {
	if i < 0 || i >= v.size {
		return nil, errOutOfBounds
	}
	base := i << 3
	offsets := v.offsets[base:]
	start := binary.BigEndian.Uint64(offsets)
	end := binary.BigEndian.Uint64(offsets[8:])
	if start > end {
		return nil, errOutOfBounds
	}

	size := end - start
	buf := make([]byte, size)
	copy(buf, v.elems[start:end])

	return buf, nil
}

func (v *varLenSortedSet) Compare(i int, val []byte) (int, error) {
	if i < 0 || i >= v.size {
		return 0, errOutOfBounds
	}
	base := i << 3
	offsets := v.offsets[base:]
	start := binary.BigEndian.Uint64(offsets)
	end := binary.BigEndian.Uint64(offsets[8:])
	if start > end {
		return 0, errOutOfBounds
	}
	return bytes.Compare(v.elems[start:end], val), nil
}

func (v *varLenSortedSet) Size() int {
	return v.size
}

func (v *varLenSortedSet) Index(val []byte) int {
	return sortedSetIndexByte(v, val)
}

// sortedSetIndexByte does a binary search of value val in SortedSet v.
func sortedSetIndexByte(v SortedSet, val []byte) int {
	start := 0
	end := v.Size() - 1
	for start <= end {
		mid := (start + end) >> 1
		idx, err := v.Compare(mid, val)
		if err != nil {
			return -1
		}
		if idx > 0 {
			end = mid - 1
		} else if idx < 0 {
			start = mid + 1
		} else {
			return idx
		}
	}
	return -1
}

type bitSetIndexToIndexMultiMap struct {
	keysCount int
	size      int
	elems     []byte
}

var _ IndexToIndexMultiMap = &bitSetIndexToIndexMultiMap{}

func NewBitSetIndexToIndexMultiMap(r io.Reader) (*bitSetIndexToIndexMultiMap, error) {
	var n uint32
	if err := readUint32(r, &n); err != nil {
		return nil, err
	}
	res := &bitSetIndexToIndexMultiMap{
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

func (m *bitSetIndexToIndexMultiMap) Get(n int, v BitSet) (bool, error) {
	if n < 0 || n >= m.keysCount {
		return false, errOutOfBounds
	}

	offsetBytes := n * (m.size << 3)
	elems := m.elems[offsetBytes:]

	// FIXME(varankinv): move to bitSet
	b, ok := v.(*bitSet)
	if !ok {
		panic("implement me")
	}

	wordSize := bitSetWordSize(uint(b.size))
	if wordSize != uint(m.size) {
		return false, errors.New("size not equal")
	}

	var (
		w        uint64
		notEmpty bool
	)
	for i := uint(0); i < wordSize; i++ {
		w, elems = binary.BigEndian.Uint64(elems), elems[8:]
		b.words[i] |= w
		if b.words[i] != 0 {
			notEmpty = true
		}
	}

	return notEmpty, nil
}

type intIndexToIndexMap struct {
	size  int
	elems []byte
}

var _ IndexToIndexMap = &intIndexToIndexMap{}

func NewIntIndexToIndexMap(r io.Reader) (*intIndexToIndexMap, error) {
	var n uint32
	if err := readUint32(r, &n); err != nil {
		return nil, err
	}
	res := &intIndexToIndexMap{
		size: int(n),
	}
	var err error
	if res.elems, err = ioutil.ReadAll(r); err != nil {
		return nil, err
	}

	return res, nil
}

func (m *intIndexToIndexMap) Get(n int) (int, error) {
	if n < 0 || n >= m.size {
		return 0, errOutOfBounds
	}
	k := uint64(n) << 2
	return int(binary.BigEndian.Uint32(m.elems[k:])), nil
}

func readUint64(r io.Reader, v *uint64) error {
	b := make([]byte, 8)
	if _, err := r.Read(b); err != nil {
		return err
	}
	*v = binary.BigEndian.Uint64(b)
	return nil
}

func readUint32(r io.Reader, v *uint32) error {
	b := make([]byte, 4)
	if _, err := r.Read(b); err != nil {
		return err
	}
	*v = binary.BigEndian.Uint32(b)
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
