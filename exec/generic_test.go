package exec

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQueryMapResultGet(t *testing.T) {
	cases := []struct {
		name   string
		row    QueryMapResult
		column string
		want   any
		found  bool
	}{
		{
			name:   "the alias comes back exactly as it was asked for",
			row:    QueryMapResult{"fail_0": int64(3)},
			column: "fail_0",
			want:   int64(3),
			found:  true,
		},
		{
			// QUOTED_IDENTIFIERS_IGNORE_CASE = TRUE: the whole point.
			name:   "the account folded the alias to upper case",
			row:    QueryMapResult{"FAIL_0": int64(3), "FAIL_ANY": int64(3)},
			column: "fail_0",
			want:   int64(3),
			found:  true,
		},
		{
			name:   "the caller asks in a case the warehouse did not use",
			row:    QueryMapResult{"fail_any": int64(7)},
			column: "FAIL_ANY",
			want:   int64(7),
			found:  true,
		},
		{
			name:   "the warehouse used mixed case",
			row:    QueryMapResult{"Num_Failures": int64(2)},
			column: "num_failures",
			want:   int64(2),
			found:  true,
		},
		{
			// A miss has to stay a miss, or an alias we got wrong reads as a
			// column that legitimately holds nothing.
			name:   "the column is not in the result set",
			row:    QueryMapResult{"FAIL_0": int64(3)},
			column: "fail_1",
			want:   nil,
			found:  false,
		},
		{
			// The evaluator ran and produced nothing, which is not the same as
			// never having been asked for.
			name:   "the column is present and NULL",
			row:    QueryMapResult{"FAIL_0": nil},
			column: "fail_0",
			want:   nil,
			found:  true,
		},
		{
			name:   "the exact spelling wins over a case variant",
			row:    QueryMapResult{"id": int64(1), "ID": int64(2)},
			column: "ID",
			want:   int64(2),
			found:  true,
		},
		{
			name:   "an empty row",
			row:    QueryMapResult{},
			column: "fail_0",
			want:   nil,
			found:  false,
		},
		{
			name:   "a nil row",
			row:    nil,
			column: "fail_0",
			want:   nil,
			found:  false,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, found := c.row.Get(c.column)
			assert.Equal(t, c.found, found)
			assert.Equal(t, c.want, got)
		})
	}
}
