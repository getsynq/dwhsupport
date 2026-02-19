package testenv

import (
	"os"
	"strconv"
)

// EnvOrDefaultBool returns the boolean value of the environment variable key, or def if unset, empty, or not parseable.
// Accepts "1", "t", "T", "TRUE", "true", "True" as true and "0", "f", "F", "FALSE", "false", "False" as false.
func EnvOrDefaultBool(key string, def bool) bool {
	if v := os.Getenv(key); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return def
}

// EnvOrDefault returns the value of the environment variable key, or def if unset or empty.
func EnvOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// EnvOrDefaultInt returns the integer value of the environment variable key, or def if unset, empty, or not a valid integer.
func EnvOrDefaultInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
