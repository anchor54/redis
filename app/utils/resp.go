package utils

import (
	"bufio"
	"bytes"
	"fmt"
	"strconv"
	"strings"

	ds "github.com/codecrafters-io/redis-starter-go/app/data-structure"
)

// parseRESPArray parses a RESP array (like *2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n)
// and returns a []string
func ParseRESPArray(resp string) ([]string, error) {
	reader := bufio.NewReader(bytes.NewBufferString(resp))

	// 1️⃣ Expect array prefix: *<count>\r\n
	prefix, err := reader.ReadString('\n')
	if err != nil {
		return nil, fmt.Errorf("invalid prefix: %w", err)
	}
	prefix = strings.TrimSpace(prefix)

	if len(prefix) == 0 || prefix[0] != '*' {
		return nil, fmt.Errorf("expected array prefix, got %q", prefix)
	}

	count, err := strconv.Atoi(prefix[1:])
	if err != nil {
		return nil, fmt.Errorf("invalid array length: %w", err)
	}

	// 2️⃣ Parse <count> bulk strings
	result := make([]string, 0, count)
	for i := 0; i < count; i++ {
		// Expect $<len>\r\n
		lenLine, err := reader.ReadString('\n')
		if err != nil {
			return nil, fmt.Errorf("error reading bulk string length: %w", err)
		}
		lenLine = strings.TrimSpace(lenLine)
		if lenLine == "" || lenLine[0] != '$' {
			return nil, fmt.Errorf("expected bulk string prefix, got %q", lenLine)
		}

		strLen, err := strconv.Atoi(lenLine[1:])
		if err != nil {
			return nil, fmt.Errorf("invalid bulk string length: %w", err)
		}

		// Read string of given length
		data := make([]byte, strLen+2) // +2 for \r\n
		if _, err := reader.Read(data); err != nil {
			return nil, fmt.Errorf("error reading bulk string data: %w", err)
		}
		result = append(result, string(data[:strLen]))
	}

	return result, nil
}

// ToSimpleString converts a Go string to a RESP Simple String.
func ToSimpleString(s string) string {
	return fmt.Sprintf("+%s\r\n", s)
}

// ToBulkString converts a Go string to a RESP Bulk String.
func ToBulkString(s string) string {
	return fmt.Sprintf("$%d\r\n%s\r\n", len(s), s)
}

// ToNullBulkString returns the RESP representation of a null bulk string.
func ToNullBulkString() string {
	return "$-1\r\n"
}

// ToRespInt returns the RESP representation of an integer value in string
func ToRespInt(n int) string {
	return fmt.Sprintf(":%d\r\n", n)
}

// ToArray converts a slice of strings to a RESP Array of Bulk Strings.
func ToArray(elements []string) string {
	if elements == nil {
		return "*-1\r\n"
	}

	result := fmt.Sprintf("*%d\r\n", len(elements))
	for _, e := range elements {
		result += ToBulkString(e)
	}
	return result
}

func ToError(s string) string {
	return fmt.Sprintf("-ERR %s\r\n", s)
}

// ToStreamEntries converts a slice of stream entries to a RESP array.
// Each entry is represented as an array of 2 elements:
//   - Element 0: The entry ID as a bulk string
//   - Element 1: An array of field key-value pairs (flattened)
func ToStreamEntries(entries []*ds.Entry) string {
	if entries == nil {
		return "*-1\r\n"
	}

	result := fmt.Sprintf("*%d\r\n", len(entries))
	for _, entry := range entries {
		// Each entry is an array of 2 elements: [ID, fields_array]
		result += "*2\r\n"
		// Element 0: ID as bulk string
		result += ToBulkString(entry.ID.String())
		// Element 1: Fields as array (key-value pairs flattened)
		fields := make([]string, 0, len(entry.Fields)*2)
		for key, value := range entry.Fields {
			fields = append(fields, key, value)
		}
		result += ToArray(fields)
	}
	return result
}
