package sqldialect

import "testing"

func TestResolveFieldRef_Snowflake(t *testing.T) {
	d := NewSnowflakeDialect()
	cases := []struct {
		in, want string
	}{
		{"ingested_at", "ingested_at"},
		{"INGESTED_AT", "INGESTED_AT"},
		{"ingestedAt", `"ingestedAt"`},
		{"Created At", `"Created At"`},
		{"_meta/mtime", `"_meta/mtime"`},
		{"payload->>'amount'", "payload->>'amount'"},
		{"value::numeric", "value::numeric"},
		{"CAST(x AS INT)", "CAST(x AS INT)"},
		{"lower(name)", "lower(name)"},
		{"coalesce(a, b)", "coalesce(a, b)"},
		{"metadata.created_at", "metadata.created_at"},
		{"things as service", `"things as service"`},
		{`"Created At"`, `"Created At"`},
		{"`col`", "\"`col`\""},
		{"[col]", `"[col]"`},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			if got := d.ResolveFieldRef(tc.in); got != tc.want {
				t.Errorf("Snowflake.ResolveFieldRef(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestResolveFieldRef_Oracle(t *testing.T) {
	d := NewOracleDialect()
	cases := []struct {
		in, want string
	}{
		{"ingested_at", "ingested_at"},
		{"INGESTED_AT", "INGESTED_AT"},
		{"ingestedAt", `"ingestedAt"`},
		{"Created At", `"Created At"`},
		{"_meta/mtime", `"_meta/mtime"`},
		{"payload->>'amount'", "payload->>'amount'"},
		{"value::numeric", "value::numeric"},
		{"CAST(x AS INT)", "CAST(x AS INT)"},
		{"metadata.created_at", "metadata.created_at"},
		{`"Created At"`, `"Created At"`},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			if got := d.ResolveFieldRef(tc.in); got != tc.want {
				t.Errorf("Oracle.ResolveFieldRef(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestResolveFieldRef_FoldLower(t *testing.T) {
	dialects := []struct {
		name string
		d    Dialect
	}{
		{"postgres", NewPostgresDialect()},
		{"redshift", NewRedshiftDialect()},
		{"trino", NewTrinoDialect()},
	}
	cases := []struct {
		in, want string
	}{
		{"ingested_at", "ingested_at"},
		{"INGESTED_AT", `"INGESTED_AT"`},
		{"ingestedAt", `"ingestedAt"`},
		{"Created At", `"Created At"`},
		{"_meta/mtime", `"_meta/mtime"`},
		{"payload->>'amount'", "payload->>'amount'"},
		{"value::numeric", "value::numeric"},
		{"CAST(x AS INT)", "CAST(x AS INT)"},
		{"metadata.created_at", "metadata.created_at"},
		{`"Created At"`, `"Created At"`},
	}
	for _, dl := range dialects {
		for _, tc := range cases {
			t.Run(dl.name+"/"+tc.in, func(t *testing.T) {
				if got := dl.d.ResolveFieldRef(tc.in); got != tc.want {
					t.Errorf("%s.ResolveFieldRef(%q) = %q, want %q", dl.name, tc.in, got, tc.want)
				}
			})
		}
	}
}

func TestResolveFieldRef_QuoteIfNeededBackticks(t *testing.T) {
	dialects := []struct {
		name string
		d    Dialect
	}{
		{"bigquery", NewBigQueryDialect()},
		{"databricks", NewDatabricksDialect()},
		{"clickhouse", NewClickHouseDialect()},
		{"mysql", NewMySQLDialect()},
	}
	cases := []struct {
		in, want string
	}{
		{"ingested_at", "ingested_at"},
		{"INGESTED_AT", "INGESTED_AT"},
		{"ingestedAt", "ingestedAt"},
		{"Created At", "`Created At`"},
		{"_meta/mtime", "`_meta/mtime`"},
		{"with`tick", "`with``tick`"},
		{"payload->>'amount'", "payload->>'amount'"},
		{"value::numeric", "value::numeric"},
		{"CAST(x AS INT)", "CAST(x AS INT)"},
		{"lower(name)", "lower(name)"},
		{"events.payload", "events.payload"},
		{"things as service", "`things as service`"},
		{"`col`", "`col`"},
		{`"col"`, "`\"col\"`"},
	}
	for _, dl := range dialects {
		for _, tc := range cases {
			t.Run(dl.name+"/"+tc.in, func(t *testing.T) {
				if got := dl.d.ResolveFieldRef(tc.in); got != tc.want {
					t.Errorf("%s.ResolveFieldRef(%q) = %q, want %q", dl.name, tc.in, got, tc.want)
				}
			})
		}
	}
}

func TestResolveFieldRef_DuckDB(t *testing.T) {
	d := NewDuckDBDialect()
	cases := []struct {
		in, want string
	}{
		{"ingested_at", "ingested_at"},
		{"INGESTED_AT", "INGESTED_AT"},
		{"Created At", `"Created At"`},
		{"_meta/mtime", `"_meta/mtime"`},
		{"payload->>'amount'", "payload->>'amount'"},
		{"CAST(x AS INT)", "CAST(x AS INT)"},
		{"metadata.created_at", "metadata.created_at"},
		{`"Created At"`, `"Created At"`},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			if got := d.ResolveFieldRef(tc.in); got != tc.want {
				t.Errorf("DuckDB.ResolveFieldRef(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestResolveFieldRef_MSSQL(t *testing.T) {
	d := NewMSSQLDialect()
	cases := []struct {
		in, want string
	}{
		{"ingested_at", "ingested_at"},
		{"INGESTED_AT", "INGESTED_AT"},
		{"Created At", "[Created At]"},
		{"with]bracket", "[with]]bracket]"},
		{"_meta/mtime", "[_meta/mtime]"},
		{"payload->>'amount'", "payload->>'amount'"},
		{"CAST(x AS INT)", "CAST(x AS INT)"},
		{"schema.table.col", "schema.table.col"},
		{"[Created At]", "[Created At]"},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			if got := d.ResolveFieldRef(tc.in); got != tc.want {
				t.Errorf("MSSQL.ResolveFieldRef(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

// TestResolveFieldRef_CommaColumnQuoted locks the behavior after dropping `,`
// from sqlExpressionMarkers: a column literally named `a, b` must be quoted,
// not passed through raw (which would emit `count(a, b)` — a silent two-arg
// call). A comma inside a real function call still passes through because `(`
// triggers the expression heuristic.
func TestResolveFieldRef_CommaColumnQuoted(t *testing.T) {
	cases := []struct {
		name           string
		d              Dialect
		commaCol       string
		commaColInExpr string
	}{
		{"snowflake", NewSnowflakeDialect(), `"a, b"`, "coalesce(a, b)"},
		{"oracle", NewOracleDialect(), `"a, b"`, "coalesce(a, b)"},
		{"postgres", NewPostgresDialect(), `"a, b"`, "coalesce(a, b)"},
		{"redshift", NewRedshiftDialect(), `"a, b"`, "coalesce(a, b)"},
		{"trino", NewTrinoDialect(), `"a, b"`, "coalesce(a, b)"},
		{"duckdb", NewDuckDBDialect(), `"a, b"`, "coalesce(a, b)"},
		{"bigquery", NewBigQueryDialect(), "`a, b`", "coalesce(a, b)"},
		{"databricks", NewDatabricksDialect(), "`a, b`", "coalesce(a, b)"},
		{"clickhouse", NewClickHouseDialect(), "`a, b`", "coalesce(a, b)"},
		{"mysql", NewMySQLDialect(), "`a, b`", "coalesce(a, b)"},
		{"mssql", NewMSSQLDialect(), "[a, b]", "coalesce(a, b)"},
	}
	for _, tc := range cases {
		t.Run(tc.name+"/column", func(t *testing.T) {
			if got := tc.d.ResolveFieldRef("a, b"); got != tc.commaCol {
				t.Errorf("%s.ResolveFieldRef(%q) = %q, want %q", tc.name, "a, b", got, tc.commaCol)
			}
		})
		t.Run(tc.name+"/expr", func(t *testing.T) {
			if got := tc.d.ResolveFieldRef("coalesce(a, b)"); got != tc.commaColInExpr {
				t.Errorf("%s.ResolveFieldRef(%q) = %q, want %q", tc.name, "coalesce(a, b)", got, tc.commaColInExpr)
			}
		})
	}
}

func TestMSSQLQuoteIdentifier_EscapesBracket(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"foo", "[foo]"},
		{"foo]bar", "[foo]]bar]"},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			if got := MSSQLQuoteIdentifier(tc.in); got != tc.want {
				t.Errorf("MSSQLQuoteIdentifier(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}
