package redshift

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNormalizeRedshiftOperation(t *testing.T) {
	cases := []struct {
		input    string
		expected string
	}{
		{"INSERT", "INSERT"},
		{"UPDATE", "UPDATE"},
		{"DELETE", "DELETE"},
		{"COPY", "COPY"},
		{"MERGE", "MERGE"},
		{"TRUNCATE", "TRUNCATE"},
		{"DDL", "OTHER"},
		{"UNLOAD", "OTHER"},
		{"SELECT", "OTHER"},
		{"", "OTHER"},
	}
	for _, tc := range cases {
		t.Run(tc.input, func(t *testing.T) {
			assert.Equal(t, tc.expected, normalizeRedshiftOperation(tc.input))
		})
	}
}
