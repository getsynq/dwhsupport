package metrics

import (
	"testing"

	dwhsql "github.com/getsynq/dwhsupport/sqldialect"
	"github.com/stretchr/testify/require"
)

// TestResolveFieldRef locks the bare-name-vs-expression heuristic used
// by NumericMetricsValuesCols / TextMetricsValuesCols / TextMetricsLengthCols.
// Bare column names get dialect-quoted. SQL expressions pass through raw.
func TestResolveFieldRef(t *testing.T) {
	pg := dwhsql.NewPostgresDialect()
	sf := dwhsql.NewSnowflakeDialect()

	cases := []struct {
		name    string
		dialect dwhsql.Dialect
		in      string
		want    string
	}{
		// Bare names — quoted via dialect.Identifier (fold-aware)
		{"pg pure lower", pg, "price", "price"},
		{"pg pure upper", pg, "CREATEDAT", `"CREATEDAT"`},
		{"pg mixed", pg, "createdAt", `"createdAt"`},
		{"pg with space", pg, "Created At", `"Created At"`},
		{"sf pure upper", sf, "PRICE", `"PRICE"`},
		{"sf with space", sf, "Created At", `"Created At"`},

		// Fivetran-style metadata column — bare name with `/`
		{"pg fivetran meta", pg, "_meta/mtime", `"_meta/mtime"`},
		{"sf fivetran meta", sf, "_meta/mtime", `"_meta/mtime"`},

		// SQL expressions — pass through raw
		{"pg json path", pg, "payload->>'amount'", "payload->>'amount'"},
		{"pg json arrow", pg, "data->'nested'", "data->'nested'"},
		{"pg cast operator", pg, "value::numeric", "value::numeric"},
		{"sf cast keyword", sf, "CAST(x AS INT)", "CAST(x AS INT)"},
		{"pg function call", pg, "lower(name)", "lower(name)"},
		{"pg multi-arg", pg, "coalesce(a, b)", "coalesce(a, b)"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := resolveFieldRef(tc.in, tc.dialect)
			require.Equal(t, tc.want, got)
		})
	}
}
