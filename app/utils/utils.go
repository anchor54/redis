package utils

import (
	"math/rand"
	"strings"

	"github.com/google/uuid"
)

func Reverse[T any](s []T) []T {
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}
	return s
}

// Contains checks if a given element exists in a slice of integers.
func Contains(slice []string, element string) bool {
	for _, v := range slice {
		if strings.EqualFold(v, element) {
			return true
		}
	}
	return false
}


func GenerateUniqueID() string {
	// Generate a standard UUID
	id := uuid.New().String()

	// Add extra random characters to reach ~40 characters
	// A standard UUID is 36 characters long. We need 4 more.
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, 4)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	extra := string(b)

	return id + extra
}