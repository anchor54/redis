package datastructure

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	radix "github.com/hashicorp/go-immutable-radix/v2"
)

type Entry struct {
	ID     StreamID
	Fields map[string]string
}

type Stream struct {
	mu      sync.Mutex
	Index   *radix.Tree[int]
	Entries *ListPack
}

func NewStream() *Stream {
	return &Stream{Entries: NewListPack(), Index: radix.New[int]()}
}

func (s *Stream) Kind() RType {
	return RStream
}

func AsStream(v RValue) (*Stream, bool) {
	if sv, ok := v.(*Stream); ok {
		return sv, true
	}
	return nil, false
}

func (s *Stream) CreateEntry(id string, values ...string) (*Entry, error) {
	var lastID *StreamID = nil
	if s.Entries.Len() > 0 {
		lastBytes, err := s.Entries.Get(-1)
		if err != nil {
			return nil, err
		}
		lastEntry, err := DecodeEntry(lastBytes)
		if err != nil {
			return nil, err
		}

		lastID = &lastEntry.ID
	}
	entryID, err := GenerateNextID(lastID, id)
	if err != nil {
		return nil, err
	}

	fields := make(map[string]string)
	for i := 0; i < len(values); i += 2 {
		fields[values[i]] = values[i+1]
	}

	entry := Entry{ID: entryID, Fields: fields}

	return &entry, nil
}

func (s *Stream) AppendEntry(entry *Entry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Entries.Append(EncodeEntry(*entry))
	s.Index, _, _ = s.Index.Insert(entry.ID.EncodeToBytes(), s.Entries.Len()-1)
}

func (s *Stream) RangeScan(start, end StreamID) ([]*Entry, error) {
	fmt.Println("RangeScan", start, end)
	s.mu.Lock()
	defer s.mu.Unlock()
	startIter, endIter := s.Index.Root().Iterator(), s.Index.Root().Iterator()
	startIter.SeekLowerBound(start.EncodeToBytes())
	endIter.SeekLowerBound(end.EncodeToBytes())

	_, startIndex, ok := startIter.Next()
	if !ok {
		return nil, errors.New("start not found")
	}
	_, endIndex, ok := endIter.Next()
	if !ok {
		return nil, errors.New("end not found")
	}

	entries := make([]*Entry, 0)
	for ;startIndex <= endIndex; startIndex++ {
		entryBytes, err := s.Entries.Get(startIndex)
		if err != nil {
			return nil, err
		}
		entry, err := DecodeEntry(entryBytes)
		if err != nil {
			return nil, err
		}
		entries = append(entries, &entry)
	}
	return entries, nil
}

func appendVarString(dst []byte, s string) []byte {
	tmp := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(tmp, uint64(len(s)))
	dst = append(dst, tmp[:n]...)
	dst = append(dst, s...)
	return dst
}

func EncodeEntry(e Entry) []byte {
	var buf []byte
	// encode ID
	buf = append(buf, e.ID.EncodeToBytes()...)
	// encode field count
	count := uint64(len(e.Fields))
	tmp := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(tmp, count)
	buf = append(buf, tmp[:n]...)
	// encode each field
	for k, v := range e.Fields {
		buf = appendVarString(buf, k)
		buf = appendVarString(buf, v)
	}
	return buf
}

func DecodeEntry(b []byte) (Entry, error) {
	var e Entry
	pos := 0

	// ID - encoded as 16 fixed bytes (8 bytes Ms + 8 bytes Seq)
	if len(b) < 16 {
		return e, errors.New("invalid entry: too short for ID")
	}
	var err error
	e.ID, err = DecodeFromBytes(b[pos : pos+16])
	if err != nil {
		return e, err
	}
	pos += 16

	// numFields
	count, n2 := binary.Uvarint(b[pos:])
	if n2 <= 0 {
		return e, errors.New("invalid field count")
	}
	pos += n2

	e.Fields = make(map[string]string, count)
	for i := uint64(0); i < count; i++ {
		k, nk, err := readVarString(b, pos)
		if err != nil {
			return e, err
		}
		pos += nk
		v, nv, err := readVarString(b, pos)
		if err != nil {
			return e, err
		}
		pos += nv
		e.Fields[k] = v
	}
	return e, nil
}

func readVarString(b []byte, pos int) (string, int, error) {
	length, n := binary.Uvarint(b[pos:])
	if n <= 0 {
		return "", 0, errors.New("invalid length varint")
	}
	start := pos + n
	end := start + int(length)
	if end > len(b) {
		return "", 0, errors.New("truncated data")
	}
	return string(b[start:end]), n + int(length), nil
}
