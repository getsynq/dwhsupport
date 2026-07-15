package stdsql

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParsePostgresArrayLiteral(t *testing.T) {
	tests := []struct {
		name     string
		literal  string
		elemType string
		want     string // JSON of the parsed tree
	}{
		{"int_array", `{1,2,3}`, "INT4", `[1,2,3]`},
		{"bigint_preserves_precision", `{9223372036854775807}`, "INT8", `[9223372036854775807]`},
		{"text_array_unquoted", `{a,b}`, "TEXT", `["a","b"]`},
		{"text_array_quoted", `{"a","b"}`, "TEXT", `["a","b"]`},
		{"quoted_with_comma", `{"a,b","c"}`, "TEXT", `["a,b","c"]`},
		{"quoted_with_escapes", `{"a\"b","c\\d"}`, "TEXT", `["a\"b","c\\d"]`},
		{"empty_array", `{}`, "INT4", `[]`},
		{"null_element", `{NULL,1}`, "INT4", `[null,1]`},
		{"quoted_null_is_string", `{"NULL"}`, "TEXT", `["NULL"]`},
		{"nested_array", `{{1,2},{3,4}}`, "INT4", `[[1,2],[3,4]]`},
		{"nested_ragged", `{{1},{2,3}}`, "INT4", `[[1],[2,3]]`},
		{"float_array", `{1.5,2.25}`, "FLOAT8", `[1.5,2.25]`},
		{"bool_array", `{t,f}`, "BOOL", `[true,false]`},
		{"numeric_stays_number", `{1.10,2.00}`, "NUMERIC", `[1.10,2.00]`},
		{"text_that_looks_numeric_stays_string", `{01,02}`, "TEXT", `["01","02"]`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tree, ok := parsePostgresArrayLiteral(tt.literal, tt.elemType)
			require.True(t, ok)
			got, err := json.Marshal(tree)
			require.NoError(t, err)
			assert.Equal(t, tt.want, string(got))
		})
	}
}

func TestParsePostgresArrayLiteral_Rejects(t *testing.T) {
	// Composite/record literals and malformed input must be rejected so the
	// caller keeps them as strings.
	for _, in := range []string{
		`(1,x)`,      // composite RECORD, not an array
		`1,2,3`,      // no braces
		`{1,2`,       // unterminated
		`{1,2}extra`, // trailing garbage
		`{"a}`,       // unterminated quote
		``,           // empty string
	} {
		t.Run(in, func(t *testing.T) {
			_, ok := parsePostgresArrayLiteral(in, "INT4")
			assert.False(t, ok)
		})
	}
}
