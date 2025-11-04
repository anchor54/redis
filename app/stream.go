package main

type Entry struct {
	ID string
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

func CreateEntry(id string, values ...string) *Entry {
	fields := make(map[string]string)
	for i := 0; i < len(values); i += 2 {
		fields[values[i]] = values[i+1]
	}
	return &Entry{ID: id, Fields: fields}
}

func (s *Stream) AppendEntry(entry *Entry) {
	s.Entries = append(s.Entries, entry)
}