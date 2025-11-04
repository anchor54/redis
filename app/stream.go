package main

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
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

// parseID parses an ID string into millisecond and sequence parts
func parseID(id string) (millisecond, seq int64, ok bool) {
	parts := strings.Split(id, "-")
	if len(parts) != 2 {
		return 0, 0, false
	}
	millisecond, err1 := strconv.ParseInt(parts[0], 10, 64)
	seq, err2 := strconv.ParseInt(parts[1], 10, 64)
	return millisecond, seq, err1 == nil && err2 == nil && millisecond >= 0 && seq >= 0
}

// getLastID returns the last entry's ID components, or (0, 0) if stream is empty
func (s *Stream) getLastID() (millisecond, seq int64) {
	if len(s.Entries) > 0 {
		if ms, sq, ok := parseID(s.Entries[len(s.Entries)-1].ID); ok {
			return ms, sq
		}
	}
	return 0, 0
}

// getMaxSeqForMillisecond finds the maximum sequence number for entries with the given millisecond
func (s *Stream) getMaxSeqForMillisecond(targetMs int64) int64 {
	maxSeq := int64(-1)
	for _, entry := range s.Entries {
		if ms, seq, ok := parseID(entry.ID); ok && ms == targetMs && seq > maxSeq {
			maxSeq = seq
		}
	}
	return maxSeq
}

func (s *Stream) GenerateEntryID(pattern string) (string, error) {
	// Handle full wildcard pattern: "*"
	if pattern == "*" {
		millisecond := time.Now().UnixMilli()
		maxSeq := s.getMaxSeqForMillisecond(millisecond)
		return fmt.Sprintf("%d-%d", millisecond, maxSeq+1), nil
	}

	// Validate pattern format
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

	lastMs, lastSeq := s.getLastID()

	// Case 1: Explicit ID (e.g., "64351354687-4")
	if seq, err := strconv.ParseInt(parts[1], 10, 64); err == nil {
		if seq < 0 {
			return "", ErrInvalidPattern
		}
		if millisecond == 0 && seq == 0 {
			return "", ErrMustBeGreaterThan00
		}
		if millisecond < lastMs || (millisecond == lastMs && seq <= lastSeq) {
			return "", ErrNotGreaterThanLastID
		}
		return pattern, nil
	}

	// Case 2: Partial ID with wildcard (e.g., "64351354687-*")
	if parts[1] != "*" {
		return "", ErrInvalidPattern
	}

	if millisecond < lastMs {
		return "", ErrNotGreaterThanLastID
	}

	seq := int64(0)
	if millisecond == lastMs {
		seq = lastSeq + 1
	}

	return fmt.Sprintf("%d-%d", millisecond, seq), nil
}
