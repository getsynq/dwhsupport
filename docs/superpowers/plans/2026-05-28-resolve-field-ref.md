# `ResolveFieldRef` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Quote user-supplied column references in metric SQL via a new dialect-level `ResolveFieldRef` method so Snowflake `"Created At"`-style names work, without churning snapshots for the common lowercase-column case.

**Architecture:** Add `ResolveFieldRef(string) string` to the `Dialect` interface. Each dialect picks one of three quoting strategies (fold-upper / fold-lower / quote-if-needed). A shared `isLikelyExpression` heuristic lets SQL expressions (json paths, casts, function calls) pass through raw. `TextColExpr.ToSql`, `NumericColExpr.ToSql`, and each dialect's `ResolveTimeColumn` all dispatch through this single method.

**Tech Stack:** Go, `github.com/gkampitakis/go-snaps`, `github.com/stretchr/testify`.

**Spec:** `docs/superpowers/specs/2026-05-28-resolve-field-ref-design.md`

---

## File Inventory

**Modified:**
- `sqldialect/helpers.go` — add `IsUpper`, `QuoteForFoldUpper`, `QuoteForFoldLower`, `QuoteWithBracketsIfNeeded`, `isLikelyExpression`
- `sqldialect/dialect.go` — add `ResolveFieldRef` to `Dialect` interface
- `sqldialect/dialect_snowflake.go` — add `ResolveFieldRef`; refactor `ResolveTimeColumn`
- `sqldialect/dialect_oracle.go` — same
- `sqldialect/dialect_postgres.go` — same
- `sqldialect/dialect_redshift.go` — same
- `sqldialect/dialect_trino.go` — same
- `sqldialect/dialect_bigquery.go` — add `ResolveFieldRef`; refactor `ResolveTimeColumn` (keep `timestamp()` wrap)
- `sqldialect/dialect_databricks.go` — same shape, no wrap
- `sqldialect/dialect_clickhouse.go` — same
- `sqldialect/dialect_mysql.go` — same
- `sqldialect/dialect_duckdb.go` — same
- `sqldialect/dialect_mssql.go` — add `ResolveFieldRef`; refactor `ResolveTimeColumn`; fix `MSSQLQuoteIdentifier` `]` escape
- `sqldialect/base.go` — override `TextColExpr.ToSql` / `NumericColExpr.ToSql`
- `metrics/**/*.snap` — regenerate (cosmetic strip on Oracle/MSSQL, new quoting on Snowflake mixed-case)
- `scrapper/databricks/__snapshots__/*.snap` — regenerate if affected

**Created:**
- `sqldialect/helpers_test.go` — unit tests for the new helpers
- `sqldialect/resolve_field_ref_test.go` — per-dialect table tests for `ResolveFieldRef`

---

## Task 1: Add helpers + helper tests

**Files:**
- Modify: `sqldialect/helpers.go`
- Create: `sqldialect/helpers_test.go`

- [ ] **Step 1: Write failing helper tests**

Create `sqldialect/helpers_test.go`:

```go
package sqldialect

import "testing"

func TestIsUpper(t *testing.T) {
	cases := []struct {
		in   string
		want bool
	}{
		{"", true},
		{"FOO", true},
		{"FOO_BAR", true},
		{"FOO123", true},
		{"foo", false},
		{"Foo", false},
		{"123", true},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			if got := IsUpper(tc.in); got != tc.want {
				t.Errorf("IsUpper(%q) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}

func TestQuoteForFoldUpper(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"ingested_at", "ingested_at"},   // pure-lower, no special chars → raw
		{"INGESTED_AT", "INGESTED_AT"},   // pure-upper → raw
		{"ingestedAt", `"ingestedAt"`},   // mixed-case → quoted
		{"Created At", `"Created At"`},   // space → quoted
		{"_meta/mtime", `"_meta/mtime"`}, // `/` → quoted
		{"a\"b", `"a""b"`},               // internal `"` → doubled
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			if got := QuoteForFoldUpper(tc.in, `"`); got != tc.want {
				t.Errorf("QuoteForFoldUpper(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestQuoteForFoldLower(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"ingested_at", "ingested_at"},   // pure-lower → raw
		{"INGESTED_AT", `"INGESTED_AT"`}, // pure-upper → quoted
		{"ingestedAt", `"ingestedAt"`},   // mixed-case → quoted
		{"Created At", `"Created At"`},   // space → quoted
		{"_meta/mtime", `"_meta/mtime"`}, // `/` → quoted
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			if got := QuoteForFoldLower(tc.in, `"`); got != tc.want {
				t.Errorf("QuoteForFoldLower(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestQuoteWithBracketsIfNeeded(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"ingested_at", "ingested_at"},   // safe → raw
		{"INGESTED_AT", "INGESTED_AT"},   // safe → raw (MSSQL is case-insensitive collation)
		{"Created At", "[Created At]"},   // space → bracketed
		{"with]bracket", "[with]]bracket]"}, // internal `]` → doubled
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			if got := QuoteWithBracketsIfNeeded(tc.in); got != tc.want {
				t.Errorf("QuoteWithBracketsIfNeeded(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestIsLikelyExpression(t *testing.T) {
	cases := []struct {
		in   string
		want bool
	}{
		{"ingested_at", false},
		{"Created At", false},
		{"_meta/mtime", false},
		{"lower(name)", true},
		{"coalesce(a, b)", true},
		{"value::numeric", true},
		{"payload->>'amount'", true},
		{"data->'nested'", true},
		{"a #>> '{b,c}'", true},
		{"CAST(x AS INT)", true},
		{"foo as bar", true},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			if got := isLikelyExpression(tc.in); got != tc.want {
				t.Errorf("isLikelyExpression(%q) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./sqldialect/ -run 'TestIsUpper|TestQuoteForFold|TestQuoteWithBracketsIfNeeded|TestIsLikelyExpression' -v`
Expected: FAIL with "undefined: IsUpper" etc.

- [ ] **Step 3: Implement helpers**

Append to `sqldialect/helpers.go` (after the existing `IsLower` function; `IsUpper` belongs alongside it):

```go
// IsUpper returns true if all letters in s are uppercase. Digits and other
// non-letter runes are ignored. The empty string is considered upper.
func IsUpper(s string) bool {
	for _, r := range s {
		if !unicode.IsUpper(r) && unicode.IsLetter(r) {
			return false
		}
	}
	return true
}
```

(Note: `IsLower` already exists in helpers.go. `IsUpper` mirrors it.)

Then append after `QuoteWithBackticks`:

```go
// QuoteForFoldUpper quotes an identifier for fold-to-upper dialects
// (Snowflake, Oracle). Leaves pure-upper AND pure-lower identifiers
// unquoted (pure-upper matches catalog canonical case directly; pure-lower
// folds up to match). Quotes mixed-case and identifiers containing
// characters requiring quoting.
// MUST NOT be used for fold-to-lower dialects (Postgres, Redshift, Trino) —
// pure-upper input would slip through unquoted and fold to lower,
// missing the catalog entry. Use QuoteForFoldLower there.
// quoteChar must be a single character (e.g. `"`).
func QuoteForFoldUpper(identifier string, quoteChar string) string {
	if needsQuoting(identifier) || (!IsUpper(identifier) && !IsLower(identifier)) {
		escaped := strings.ReplaceAll(identifier, quoteChar, quoteChar+quoteChar)
		return quoteChar + escaped + quoteChar
	}
	return identifier
}

// QuoteForFoldLower quotes an identifier for fold-to-lower dialects
// (Postgres, Redshift, Trino). Leaves only pure-lower identifiers with no
// characters requiring quoting unquoted. Any uppercase letter or special
// char would otherwise fold or parse incorrectly.
// MUST NOT be used for fold-to-upper dialects (Snowflake, Oracle) —
// pure-upper input would be needlessly quoted. Use QuoteForFoldUpper there.
// quoteChar must be a single character (e.g. `"`).
func QuoteForFoldLower(identifier string, quoteChar string) string {
	if !needsQuoting(identifier) && IsLower(identifier) {
		return identifier
	}
	escaped := strings.ReplaceAll(identifier, quoteChar, quoteChar+quoteChar)
	return quoteChar + escaped + quoteChar
}

// QuoteWithBracketsIfNeeded quotes an identifier with [brackets] (MSSQL syntax)
// only when needed. Returns the identifier raw when it contains only safe chars.
// Internal `]` characters are escaped by doubling.
func QuoteWithBracketsIfNeeded(identifier string) string {
	if !needsQuoting(identifier) {
		return identifier
	}
	escaped := strings.ReplaceAll(identifier, "]", "]]")
	return fmt.Sprintf("[%s]", escaped)
}

// sqlExpressionMarkers are substrings whose presence in a field string
// indicates a SQL expression rather than a bare column name. Used by
// each dialect's ResolveFieldRef to skip identifier quoting for
// expressions while still quoting catalog column names that contain
// special chars (e.g. Fivetran-style "_meta/mtime").
var sqlExpressionMarkers = []string{
	"(", ")",
	"->>", "->",
	"#>>", "#>",
	"::",
	",",
	" AS ", " as ",
}

// isLikelyExpression returns true when the string contains a substring
// that strongly suggests a SQL expression (function call, json path, cast,
// multi-arg list, AS clause). Heuristic — see sqlExpressionMarkers.
func isLikelyExpression(s string) bool {
	for _, m := range sqlExpressionMarkers {
		if strings.Contains(s, m) {
			return true
		}
	}
	return false
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./sqldialect/ -run 'TestIsUpper|TestQuoteForFold|TestQuoteWithBracketsIfNeeded|TestIsLikelyExpression' -v`
Expected: PASS for all subtests.

- [ ] **Step 5: Run full sqldialect test suite to verify no regressions**

Run: `go test ./sqldialect/... -count=1`
Expected: PASS (no snapshot diffs yet; only helpers added).

- [ ] **Step 6: Commit**

```bash
git add sqldialect/helpers.go sqldialect/helpers_test.go
git commit -m "sqldialect: add fold-aware identifier-quoting helpers"
```

---

## Task 2: Add `ResolveFieldRef` stubs on every dialect (build stays green)

This task adds a stub `ResolveFieldRef` on each dialect that delegates to `Identifier`, so the interface change in Task 3 won't break the build. Subsequent tasks replace each stub with the correct strategy.

**Files:**
- Modify: `sqldialect/dialect_bigquery.go`, `dialect_clickhouse.go`, `dialect_databricks.go`, `dialect_duckdb.go`, `dialect_mssql.go`, `dialect_mysql.go`, `dialect_oracle.go`, `dialect_postgres.go`, `dialect_redshift.go`, `dialect_snowflake.go`, `dialect_trino.go`

- [ ] **Step 1: Add stub method to each dialect**

Insert immediately after each dialect's `Identifier` method (line ~88 in most files):

```go
// ResolveFieldRef returns the SQL reference for a user-supplied field name.
// Stub — delegates to Identifier. Replaced with the dialect's strategy in a follow-up task.
func (d *SnowflakeDialect) ResolveFieldRef(name string) string {
	return d.Identifier(name)
}
```

Adjust receiver type per file (`*SnowflakeDialect`, `*OracleDialect`, `*PostgresDialect`, `*RedshiftDialect`, `*TrinoDialect`, `*BigQueryDialect`, `*DatabricksDialect`, `*ClickHouseDialect`, `*MySQLDialect`, `*DuckDBDialect`, `*MSSQLDialect`).

- [ ] **Step 2: Verify build still passes**

Run: `go build ./...`
Expected: PASS — every dialect now has the method as an ordinary public method (not yet in the interface).

- [ ] **Step 3: Commit**

```bash
git add sqldialect/dialect_*.go
git commit -m "sqldialect: add ResolveFieldRef stubs delegating to Identifier"
```

---

## Task 3: Add `ResolveFieldRef` to the `Dialect` interface

**Files:**
- Modify: `sqldialect/dialect.go:24`

- [ ] **Step 1: Add interface method**

In `sqldialect/dialect.go`, add to the `Dialect` interface (alongside `Identifier`):

```go
Identifier(string) string
ResolveFieldRef(string) string
StringLiteral(string) string
```

- [ ] **Step 2: Verify build still passes**

Run: `go build ./...`
Expected: PASS — every dialect already has the stub from Task 2.

- [ ] **Step 3: Commit**

```bash
git add sqldialect/dialect.go
git commit -m "sqldialect: add ResolveFieldRef to Dialect interface"
```

---

## Task 4: Snowflake — real `ResolveFieldRef` (fold-upper)

**Files:**
- Modify: `sqldialect/dialect_snowflake.go`
- Create/Modify: `sqldialect/resolve_field_ref_test.go`

- [ ] **Step 1: Write failing test**

Create `sqldialect/resolve_field_ref_test.go`:

```go
package sqldialect

import "testing"

func TestResolveFieldRef_Snowflake(t *testing.T) {
	d := NewSnowflakeDialect()
	cases := []struct {
		in, want string
	}{
		{"ingested_at", "ingested_at"},   // pure-lower → raw (folds up to canonical)
		{"INGESTED_AT", "INGESTED_AT"},   // pure-upper → raw
		{"ingestedAt", `"ingestedAt"`},   // mixed-case → quoted
		{"Created At", `"Created At"`},   // whitespace → quoted
		{"_meta/mtime", `"_meta/mtime"`}, // `/` → quoted
		{"payload->>'amount'", "payload->>'amount'"}, // expression → raw
		{"value::numeric", "value::numeric"},
		{"CAST(x AS INT)", "CAST(x AS INT)"},
		{"lower(name)", "lower(name)"},
		{"coalesce(a, b)", "coalesce(a, b)"},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			if got := d.ResolveFieldRef(tc.in); got != tc.want {
				t.Errorf("Snowflake.ResolveFieldRef(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./sqldialect/ -run TestResolveFieldRef_Snowflake -v`
Expected: FAIL — stub returns `"ingested_at"` (always-quote), test expects `ingested_at`.

- [ ] **Step 3: Replace stub with real implementation**

In `sqldialect/dialect_snowflake.go`, replace the stub `ResolveFieldRef`:

```go
func (d *SnowflakeDialect) ResolveFieldRef(name string) string {
	if isLikelyExpression(name) {
		return name
	}
	return QuoteForFoldUpper(name, `"`)
}
```

Also refactor `ResolveTimeColumn` to delegate (no behavior change today since current impl returns `expr.name` raw, but routing through `ResolveFieldRef` is the spec):

```go
func (d *SnowflakeDialect) ResolveTimeColumn(expr *TimeColExpr) (string, error) {
	return d.ResolveFieldRef(expr.name), nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./sqldialect/ -run TestResolveFieldRef_Snowflake -v`
Expected: PASS for all subtests.

- [ ] **Step 5: Run full sqldialect suite — expect no snapshot diff yet**

Run: `go test ./sqldialect/... -count=1`
Expected: PASS. (TextCol/NumericCol still bypass `ResolveFieldRef`; switchover happens in Task 10. TimeCol on Snowflake produces the same output as before for the same inputs in current snapshots.)

- [ ] **Step 6: Commit**

```bash
git add sqldialect/dialect_snowflake.go sqldialect/resolve_field_ref_test.go
git commit -m "sqldialect: snowflake ResolveFieldRef uses fold-upper quoting"
```

---

## Task 5: Oracle — real `ResolveFieldRef` (fold-upper)

**Files:**
- Modify: `sqldialect/dialect_oracle.go`, `sqldialect/resolve_field_ref_test.go`

- [ ] **Step 1: Add Oracle test in `resolve_field_ref_test.go`**

Append to `sqldialect/resolve_field_ref_test.go`:

```go
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
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			if got := d.ResolveFieldRef(tc.in); got != tc.want {
				t.Errorf("Oracle.ResolveFieldRef(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./sqldialect/ -run TestResolveFieldRef_Oracle -v`
Expected: FAIL — stub returns `"ingested_at"` (Oracle Identifier is unconditional).

- [ ] **Step 3: Replace stub + refactor ResolveTimeColumn**

In `sqldialect/dialect_oracle.go`:

```go
func (d *OracleDialect) ResolveFieldRef(name string) string {
	if isLikelyExpression(name) {
		return name
	}
	return QuoteForFoldUpper(name, `"`)
}

func (d *OracleDialect) ResolveTimeColumn(expr *TimeColExpr) (string, error) {
	return d.ResolveFieldRef(expr.name), nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./sqldialect/ -run TestResolveFieldRef_Oracle -v`
Expected: PASS.

- [ ] **Step 5: Run full sqldialect + metrics suites — expect Oracle snapshot diffs**

Run: `UPDATE_SNAPS=true go test ./sqldialect/... ./metrics/... -count=1`
Expected: PASS. Oracle TimeCol snapshots previously had `"ingested_at"` via unconditional `OracleQuoteIdentifier`; now `ingested_at` raw. Inspect the diff:

```bash
git diff -- sqldialect/ metrics/
```

Verify diff only affects Oracle, and only strips cosmetic quotes around lowercase column refs in `TimeCol`-derived expressions. No diff for any other dialect.

- [ ] **Step 6: Commit**

```bash
git add sqldialect/dialect_oracle.go sqldialect/resolve_field_ref_test.go metrics/ sqldialect/
git commit -m "sqldialect: oracle ResolveFieldRef uses fold-upper quoting"
```

---

## Task 6: Postgres, Redshift, Trino — real `ResolveFieldRef` (fold-lower)

These three share the same impl shape (`QuoteForFoldLower`). One task per dialect would be repetitive — bundle them but run the tests separately.

**Files:**
- Modify: `sqldialect/dialect_postgres.go`, `dialect_redshift.go`, `dialect_trino.go`, `sqldialect/resolve_field_ref_test.go`

- [ ] **Step 1: Add tests for all three**

Append to `sqldialect/resolve_field_ref_test.go`:

```go
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
		{"INGESTED_AT", `"INGESTED_AT"`}, // pure-upper → quoted (fold-lower would corrupt)
		{"ingestedAt", `"ingestedAt"`},
		{"Created At", `"Created At"`},
		{"_meta/mtime", `"_meta/mtime"`},
		{"payload->>'amount'", "payload->>'amount'"},
		{"value::numeric", "value::numeric"},
		{"CAST(x AS INT)", "CAST(x AS INT)"},
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./sqldialect/ -run TestResolveFieldRef_FoldLower -v`
Expected: FAIL — stubs still delegate to `Identifier` which behaves differently than fold-lower for some inputs.

- [ ] **Step 3: Replace stubs**

In `sqldialect/dialect_postgres.go`:

```go
func (d *PostgresDialect) ResolveFieldRef(name string) string {
	if isLikelyExpression(name) {
		return name
	}
	return QuoteForFoldLower(name, `"`)
}

func (d *PostgresDialect) ResolveTimeColumn(expr *TimeColExpr) (string, error) {
	return d.ResolveFieldRef(expr.name), nil
}
```

(Note: this replaces the current `PqQuoteIdentifierIfUpper`-based `ResolveTimeColumn`. The behavior is equivalent for current snapshot inputs — verified in the design spec.)

In `sqldialect/dialect_redshift.go`:

```go
func (d *RedshiftDialect) ResolveFieldRef(name string) string {
	if isLikelyExpression(name) {
		return name
	}
	return QuoteForFoldLower(name, `"`)
}

func (d *RedshiftDialect) ResolveTimeColumn(expr *TimeColExpr) (string, error) {
	return d.ResolveFieldRef(expr.name), nil
}
```

In `sqldialect/dialect_trino.go`:

```go
func (d *TrinoDialect) ResolveFieldRef(name string) string {
	if isLikelyExpression(name) {
		return name
	}
	return QuoteForFoldLower(name, `"`)
}

func (d *TrinoDialect) ResolveTimeColumn(expr *TimeColExpr) (string, error) {
	return d.ResolveFieldRef(expr.name), nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./sqldialect/ -run TestResolveFieldRef_FoldLower -v`
Expected: PASS.

- [ ] **Step 5: Run full suites — expect no snapshot diff**

Run: `go test ./sqldialect/... ./metrics/... -count=1`
Expected: PASS, no snapshot diffs. (Postgres `PqQuoteIdentifierIfUpper` and `QuoteForFoldLower` produce identical output on snapshot inputs; Redshift/Trino TimeCol previously routed through fold-lower-equivalent helpers.)

If there ARE diffs, regenerate and inspect:

```bash
UPDATE_SNAPS=true go test ./sqldialect/... ./metrics/... -count=1
git diff -- sqldialect/ metrics/
```

Expected categories: only cosmetic strip on PG/Redshift/Trino lowercase column TimeCols.

- [ ] **Step 6: Commit**

```bash
git add sqldialect/dialect_postgres.go sqldialect/dialect_redshift.go sqldialect/dialect_trino.go sqldialect/resolve_field_ref_test.go metrics/ sqldialect/
git commit -m "sqldialect: postgres/redshift/trino ResolveFieldRef uses fold-lower quoting"
```

---

## Task 7: BigQuery, Databricks, ClickHouse, MySQL — real `ResolveFieldRef` (quote-if-needed, backticks)

**Files:**
- Modify: `sqldialect/dialect_bigquery.go`, `dialect_databricks.go`, `dialect_clickhouse.go`, `dialect_mysql.go`, `sqldialect/resolve_field_ref_test.go`

- [ ] **Step 1: Add tests**

Append to `sqldialect/resolve_field_ref_test.go`:

```go
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./sqldialect/ -run TestResolveFieldRef_QuoteIfNeededBackticks -v`
Expected: FAIL — stubs for Databricks (unconditional Identifier) return `` `ingested_at` `` instead of raw.

- [ ] **Step 3: Replace stubs**

In each of `dialect_bigquery.go`, `dialect_databricks.go`, `dialect_clickhouse.go`, `dialect_mysql.go`:

```go
func (d *<DialectType>) ResolveFieldRef(name string) string {
	if isLikelyExpression(name) {
		return name
	}
	return QuoteWithBackticks(name)
}
```

(Use the conditional `QuoteWithBackticks` from `helpers.go` even for dialects whose `Identifier` is currently unconditional — that intentional difference is what gives `ResolveFieldRef` its smart behavior without changing `Identifier`'s contract.)

Refactor each dialect's `ResolveTimeColumn`:

- BigQuery — KEEP the `timestamp(...)` wrap:

```go
func (d *BigQueryDialect) ResolveTimeColumn(expr *TimeColExpr) (string, error) {
	return fmt.Sprintf("timestamp(%s)", d.ResolveFieldRef(expr.name)), nil
}
```

- Databricks, ClickHouse, MySQL — direct delegate:

```go
func (d *<DialectType>) ResolveTimeColumn(expr *TimeColExpr) (string, error) {
	return d.ResolveFieldRef(expr.name), nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./sqldialect/ -run TestResolveFieldRef_QuoteIfNeededBackticks -v`
Expected: PASS.

- [ ] **Step 5: Run full suites — verify no unexpected snapshot diffs**

Run: `go test ./sqldialect/... ./metrics/... -count=1`
Expected: PASS, no diffs. (TimeCol on these dialects previously returned `expr.name` raw; `QuoteWithBackticks("ingested_at")` also returns raw since `needsQuoting` is false. For BigQuery, current snap is `timestamp(ingested_at)` — new output identical.)

If diffs surface, regen and inspect — expected categories: NONE for this task.

- [ ] **Step 6: Commit**

```bash
git add sqldialect/dialect_bigquery.go sqldialect/dialect_databricks.go sqldialect/dialect_clickhouse.go sqldialect/dialect_mysql.go sqldialect/resolve_field_ref_test.go
git commit -m "sqldialect: bigquery/databricks/clickhouse/mysql ResolveFieldRef quotes-if-needed (backticks)"
```

---

## Task 8: DuckDB — real `ResolveFieldRef` (quote-if-needed, double quotes)

**Files:**
- Modify: `sqldialect/dialect_duckdb.go`, `sqldialect/resolve_field_ref_test.go`

- [ ] **Step 1: Add test**

Append to `sqldialect/resolve_field_ref_test.go`:

```go
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
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			if got := d.ResolveFieldRef(tc.in); got != tc.want {
				t.Errorf("DuckDB.ResolveFieldRef(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./sqldialect/ -run TestResolveFieldRef_DuckDB -v`
Expected: PASS for some cases (DuckDB's `Identifier` is `QuoteWithDoubleQuotes` already conditional), FAIL for whatever case the stub-vs-real differs on. If all pass already, great — proceed.

- [ ] **Step 3: Replace stub**

In `sqldialect/dialect_duckdb.go`:

```go
func (d *DuckDBDialect) ResolveFieldRef(name string) string {
	if isLikelyExpression(name) {
		return name
	}
	return QuoteWithDoubleQuotes(name)
}

func (d *DuckDBDialect) ResolveTimeColumn(expr *TimeColExpr) (string, error) {
	return d.ResolveFieldRef(expr.name), nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./sqldialect/ -run TestResolveFieldRef_DuckDB -v`
Expected: PASS.

- [ ] **Step 5: Run full suites**

Run: `go test ./sqldialect/... ./metrics/... -count=1`
Expected: PASS, no diffs.

- [ ] **Step 6: Commit**

```bash
git add sqldialect/dialect_duckdb.go sqldialect/resolve_field_ref_test.go
git commit -m "sqldialect: duckdb ResolveFieldRef quotes-if-needed (double quotes)"
```

---

## Task 9: MSSQL — real `ResolveFieldRef` + internal `]` escape fix

**Files:**
- Modify: `sqldialect/dialect_mssql.go`, `sqldialect/resolve_field_ref_test.go`

- [ ] **Step 1: Add tests — ResolveFieldRef + MSSQLQuoteIdentifier escape**

Append to `sqldialect/resolve_field_ref_test.go`:

```go
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
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			if got := d.ResolveFieldRef(tc.in); got != tc.want {
				t.Errorf("MSSQL.ResolveFieldRef(%q) = %q, want %q", tc.in, got, tc.want)
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./sqldialect/ -run 'TestResolveFieldRef_MSSQL|TestMSSQLQuoteIdentifier_EscapesBracket' -v`
Expected: FAIL — stub returns `[ingested_at]`, and `MSSQLQuoteIdentifier("foo]bar")` produces `[foo]bar]` (broken — no escape).

- [ ] **Step 3: Fix `MSSQLQuoteIdentifier` + replace stub + refactor ResolveTimeColumn**

In `sqldialect/dialect_mssql.go`:

```go
func (d *MSSQLDialect) ResolveFieldRef(name string) string {
	if isLikelyExpression(name) {
		return name
	}
	return QuoteWithBracketsIfNeeded(name)
}

func (d *MSSQLDialect) ResolveTimeColumn(expr *TimeColExpr) (string, error) {
	return d.ResolveFieldRef(expr.name), nil
}
```

Update `MSSQLQuoteIdentifier`:

```go
func MSSQLQuoteIdentifier(identifier string) string {
	escaped := strings.ReplaceAll(identifier, "]", "]]")
	return fmt.Sprintf("[%s]", escaped)
}
```

Add `"strings"` to the imports in `dialect_mssql.go` if not already present.

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./sqldialect/ -run 'TestResolveFieldRef_MSSQL|TestMSSQLQuoteIdentifier_EscapesBracket' -v`
Expected: PASS.

- [ ] **Step 5: Run full suites — expect MSSQL snapshot diffs**

Run: `UPDATE_SNAPS=true go test ./sqldialect/... ./metrics/... -count=1`
Expected: PASS. MSSQL TimeCol snapshots previously had `[ingested_at]` via unconditional `MSSQLQuoteIdentifier`; now `ingested_at` raw. Inspect:

```bash
git diff -- sqldialect/ metrics/
```

Verify diff only affects MSSQL TimeCol-derived expressions and only strips cosmetic brackets around lowercase column refs.

- [ ] **Step 6: Commit**

```bash
git add sqldialect/dialect_mssql.go sqldialect/resolve_field_ref_test.go metrics/ sqldialect/
git commit -m "sqldialect: mssql ResolveFieldRef quotes-if-needed + escape internal ] in bracketed identifiers"
```

---

## Task 10: Switch `TextColExpr` / `NumericColExpr` to dispatch through `ResolveFieldRef`

This is the change that actually fixes the user-reported Snowflake `"Created At"` bug. Expect snapshot diffs across `metrics/`.

**Files:**
- Modify: `sqldialect/base.go`

- [ ] **Step 1: Override `ToSql` on both column types**

In `sqldialect/base.go`, after the existing `TextColExpr` and `NumericColExpr` definitions, add:

```go
func (s *TextColExpr) ToSql(dialect Dialect) (string, error) {
	return dialect.ResolveFieldRef(s.sql), nil
}

func (s *NumericColExpr) ToSql(dialect Dialect) (string, error) {
	return dialect.ResolveFieldRef(s.sql), nil
}
```

These override the embedded `ColBaseExpr.ToSql` (which still returns `s.sql` raw — keep it for any other future consumer).

- [ ] **Step 2: Run sqldialect + metrics tests — expect failures from snapshot mismatches**

Run: `go test ./sqldialect/... ./metrics/... -count=1`
Expected: FAIL on metrics snapshot tests. Snowflake `count(ingested_at)` stays the same; Oracle `COUNT("ingested_at")` → `COUNT(ingested_at)`; MSSQL `COUNT([ingested_at])` → `COUNT(ingested_at)`.

- [ ] **Step 3: Regenerate snapshots**

Run: `UPDATE_SNAPS=true go test ./sqldialect/... ./metrics/... -count=1`
Expected: PASS — snapshots rewritten.

- [ ] **Step 4: Inspect snapshot diff carefully**

```bash
git diff --stat -- metrics/ sqldialect/ scrapper/
git diff -- metrics/ | head -200
```

Verify each change falls into ONE of these categories:
- Oracle `COUNT("colname")` → `COUNT(colname)` (cosmetic strip on lowercase).
- MSSQL `COUNT([colname])` → `COUNT(colname)` (cosmetic strip on lowercase).
- No-op on Snowflake / BigQuery / Databricks / ClickHouse / MySQL / DuckDB / Postgres / Redshift / Trino lowercase column refs.

If any diff falls outside those categories (e.g. quoting added where it wasn't before on a lowercase name), STOP and investigate — likely a strategy bug.

- [ ] **Step 5: Run full repo build + tests**

Run: `go build ./...`
Expected: PASS.

Run: `go test ./... -count=1`
Expected: PASS or skips for integration tests requiring credentials.

- [ ] **Step 6: Commit**

```bash
git add sqldialect/base.go metrics/ sqldialect/ scrapper/
git commit -m "sqldialect: TextCol/NumericCol dispatch through ResolveFieldRef for user-supplied column names"
```

---

## Task 11: Integration / manual verification — Snowflake `"Created At"` bug

This task confirms the originally-reported bug is actually fixed. No code change — just a sanity check.

- [ ] **Step 1: Build a one-off SQL-generation test**

Add to `metrics/queries_test.go` (inside `TestMetricsSuite`). `metrics/queries_test.go` imports `dwhsql "github.com/getsynq/dwhsupport/sqldialect"` and `querybuilder`:

```go
func (s *MetricsSuite) TestNumericMetricsValuesCols_CaseSensitiveColumn() {
	dialect := dwhsql.NewSnowflakeDialect()
	cols := NumericMetricsValuesCols("Price After", dialect, WithPrefixForColumn("Price After"))
	qb := querybuilder.NewQueryBuilder(dwhsql.TableFqn("db", "sch", "tbl"), cols)
	sql, err := qb.ToSql(dialect)
	s.Require().NoError(err)
	s.T().Logf("SQL: %s", sql)
	s.Require().Contains(sql, `count("Price After")`)
	s.Require().NotContains(sql, `count(Price After)`)
}
```

- [ ] **Step 2: Run the test**

Run: `go test ./metrics/ -run 'TestMetricsSuite/TestNumericMetricsValuesCols_CaseSensitiveColumn' -v`
Expected: PASS — the generated SQL contains `count("Price After")` not `count(Price After)`.

- [ ] **Step 3: Commit**

```bash
git add metrics/queries_test.go
git commit -m "metrics: cover snowflake case-sensitive column quoting in NumericMetricsValuesCols"
```

---

## Task 12: Final review pass + open PR

- [ ] **Step 1: Diff against main and review**

```bash
git log --oneline main..HEAD
git diff main..HEAD --stat
git diff main..HEAD -- ':!*.snap' | less
```

Verify:
- `Dialect` interface gained exactly one method (`ResolveFieldRef`).
- Each of 11 dialects has `ResolveFieldRef` implemented + `ResolveTimeColumn` delegating to it.
- Helpers in `sqldialect/helpers.go` are present with doc comments.
- No leftover stubs from Task 2 (each stub replaced in Tasks 4–9).
- `TextColExpr` / `NumericColExpr` `ToSql` overrides present.
- `MSSQLQuoteIdentifier` escapes `]`.
- No changes outside `sqldialect/`, `metrics/`, and possibly `scrapper/databricks/__snapshots__/`.

- [ ] **Step 2: Run the full test matrix one more time**

Run: `go test ./... -count=1`
Expected: PASS.

- [ ] **Step 3: Format**

Run: `golines -w -m 150 sqldialect/ metrics/`
Expected: silent (or minor reformatting).

If `golines` reformatted anything, commit:

```bash
git add -u && git commit -m "format: golines"
```

- [ ] **Step 4: Open PR**

```bash
gh pr create --title "metrics, sqldialect: dialect-level ResolveFieldRef for user-supplied column names" --body "$(cat <<'EOF'
## Summary

- Adds `Dialect.ResolveFieldRef(name string) string` — fold-aware identifier quoting + SQL-expression passthrough heuristic.
- `TextCol`, `NumericCol`, and per-dialect `ResolveTimeColumn` all dispatch through it. Fixes Snowflake `"Created At"`-style column names without changing snapshots for the common lowercase-column case.
- Supersedes #186 (closes once this lands). Reuses `QuoteForFoldUpper` / `QuoteForFoldLower` helpers from that PR; drops the metrics-layer `resolveFieldRef` in favor of dialect dispatch.
- Side fix: MSSQL `MSSQLQuoteIdentifier` now escapes internal `]` by doubling.

Spec: `docs/superpowers/specs/2026-05-28-resolve-field-ref-design.md`

## Snapshot diff

- Oracle / MSSQL: lowercase column refs lose cosmetic quoting (`COUNT("ingested_at")` → `COUNT(ingested_at)`; `COUNT([ingested_at])` → `COUNT(ingested_at)`).
- All other dialects on lowercase column refs: unchanged.
- Snowflake / others on mixed-case / whitespace column refs: NEW quoting (bug fix).

## Test plan

- [x] `go test ./sqldialect/... ./metrics/... -count=1` green
- [x] `go build ./...` green
- [x] Manual: `NumericMetricsValuesCols("Price After", snowflake)` emits `count("Price After")`
- [ ] Downstream callers (kernel-anomalies, synq-monitors, kernel-ext-dwh-metrics): bump dwhsupport and verify monitor SQL for known case-sensitive columns
EOF
)"
```

- [ ] **Step 5: Close PR 186**

```bash
gh pr close 186 --comment "Superseded by the new PR — same goal, dialect-level dispatch avoids the snapshot churn."
```

---

## Spec-coverage self-check

| Spec section | Tasks |
|---|---|
| New `Dialect.ResolveFieldRef` | 3 |
| Quoting strategy table (fold-upper / fold-lower / quote-if-needed × 3 quote chars) | 4, 5, 6, 7, 8, 9 |
| Shared `isLikelyExpression` | 1 |
| `TextColExpr` / `NumericColExpr` dispatch | 10 |
| `ResolveTimeColumn` delegates to `ResolveFieldRef` (BigQuery keeps `timestamp()` wrap) | 4, 5, 6, 7, 8, 9 |
| MSSQL `]` escape fix | 9 |
| `Identifier` unchanged | n/a — verified by leaving it alone |
| Snapshot regen + diff inspection | 5, 9, 10, 12 |
| Test plan table (per-dialect inputs) | 4, 5, 6, 7, 8, 9 |
| Breaking changes documented | 12 (PR body) |
| Migration path (cherry-pick helpers, etc.) | 1 (helpers), then per-dialect tasks |
