package stdsql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCellInt64(t *testing.T) {
	cases := []struct {
		name string
		in   any
		want int64
		ok   bool
	}{
		{"int64", int64(42), 42, true},
		{"uint64", uint64(42), 42, true},
		{"int", 42, 42, true},
		{"float64", float64(42.9), 42, true},
		{"string int", "1000", 1000, true},
		{"string float", "1000.5", 1000, true},
		{"bytes", []byte("250"), 250, true},
		{"nil", nil, 0, false},
		{"non-numeric string", "abc", 0, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, ok := CellInt64(c.in)
			assert.Equal(t, c.ok, ok)
			if c.ok {
				assert.Equal(t, c.want, got)
			}
		})
	}
}

func TestCellString(t *testing.T) {
	got, ok := CellString([]byte(`{"a":1}`))
	assert.True(t, ok)
	assert.Equal(t, `{"a":1}`, got)

	got, ok = CellString("plain")
	assert.True(t, ok)
	assert.Equal(t, "plain", got)

	_, ok = CellString(nil)
	assert.False(t, ok)
}
