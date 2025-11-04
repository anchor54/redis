package main

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

var (
	ErrInvalidPattern       = errors.New("invalid pattern")
	ErrNotGreaterThanLastID = errors.New("The ID specified in XADD is equal or smaller than the target stream top item")
	ErrMustBeGreaterThan00  = errors.New("The ID specified in XADD must be greater than 0-0")
)

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

func (s *Stream) GenerateEntryID(pattern string) (string, error) {
	if !strings.Contains(pattern, "-") {
		return "", ErrInvalidPattern
	}

	parts := strings.Split(pattern, "-")
	if len(parts) != 2 {
		return "", ErrInvalidPattern
	}

	// Parse and validate millisecond part
	millisecond, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil || millisecond < 0 {
		return "", ErrInvalidPattern
	}

	// Get last ID or default to "0-0"
	lastMillisecond, lastSeq := int64(0), int64(0)
	if len(s.Entries) > 0 {
		lastParts := strings.Split(s.Entries[len(s.Entries)-1].ID, "-")
		if len(lastParts) == 2 {
			lastMillisecond, _ = strconv.ParseInt(lastParts[0], 10, 64)
			lastSeq, _ = strconv.ParseInt(lastParts[1], 10, 64)
		}
	}

	// Case 1: Explicit ID (e.g., "64351354687-4")
	if seq, err := strconv.ParseInt(parts[1], 10, 64); err == nil {
		if seq < 0 {
			return "", ErrInvalidPattern
		}
		// Check if ID is greater than 0-0
		if millisecond == 0 && seq == 0 {
			return "", ErrMustBeGreaterThan00
		}
		// Check if ID is strictly greater than last ID
		if millisecond < lastMillisecond || (millisecond == lastMillisecond && seq <= lastSeq) {
			return "", ErrNotGreaterThanLastID
		}
		return pattern, nil
	}

	// Case 2: Partial ID with wildcard (e.g., "64351354687-*")
	if parts[1] != "*" {
		return "", ErrInvalidPattern
	}

	// Validate that millisecond is not less than last millisecond
	if millisecond < lastMillisecond {
		return "", ErrNotGreaterThanLastID
	}

	// Generate sequence number
	seq := int64(0)
	if millisecond == lastMillisecond {
		seq = lastSeq + 1
	}

	return fmt.Sprintf("%d-%d", millisecond, seq), nil
}
