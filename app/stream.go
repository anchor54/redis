package main

import (
	"errors"
	"strconv"
	"strings"
)

var ID_NOT_STRICTLY_GREATER_ERROR = "The ID specified in XADD is equal or smaller than the target stream top item"
var ID_MUST_BE_GREATER_THAN_0_0_ERROR = "The ID specified in XADD must be greater than 0-0"

type Entry struct {
	ID     string
	Fields map[string]string
}

type Stream struct {
	Entries []*Entry
}

func NewStream() *Stream {
	return &Stream{Entries: make([]*Entry, 0)}
}

func (s *Stream) kind() RType {
	return RStream
}

func AsStream(v RValue) (*Stream, bool) {
	if sv, ok := v.(*Stream); ok {
		return sv, true
	}
	return nil, false
}

func (s *Stream) CreateEntry(id string, values ...string) (*Entry, error) {
	id, err := s.GenerateEntryID(id)
	if err != nil {
		return nil, err
	}

	fields := make(map[string]string)
	for i := 0; i < len(values); i += 2 {
		fields[values[i]] = values[i+1]
	}
	return &Entry{ID: id, Fields: fields}, nil
}

func (s *Stream) AppendEntry(entry *Entry) {
	s.Entries = append(s.Entries, entry)
}

func verifyExplicitID(id string) bool {
	if !strings.Contains(id, "-") {
		return false
	}
	parts := strings.Split(id, "-")
	if len(parts) != 2 {
		return false
	}
	millisecond, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return false
	}
	seq, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return false
	}
	if millisecond < 0 || seq < 0 {
		return false
	}
	return true
}

func (s *Stream) GenerateEntryID(pattern string) (string, error) {
	// case 1: explicit id (64351354687-4)
	if verifyExplicitID(pattern) {
		// check if the id is greater than 0-0
		if pattern <= "0-0" {
			return "", errors.New(ID_MUST_BE_GREATER_THAN_0_0_ERROR)
		}

		// check if the id is strictly greater than the last id in the stream
		last_id := "0-0"
		if len(s.Entries) > 0 {
			last_id = s.Entries[len(s.Entries)-1].ID
		}

		if last_id >= pattern {
			return "", errors.New(ID_NOT_STRICTLY_GREATER_ERROR)
		}

		return pattern, nil
	}

	return "", errors.New("invalid pattern")
}
