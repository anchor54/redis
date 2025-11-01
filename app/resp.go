package main

import (
	"bufio"
	"bytes"
	"fmt"
	"strconv"
	"strings"
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

func ToRespString(s string) string {
	return fmt.Sprintf("+%s\r\n", s)
}