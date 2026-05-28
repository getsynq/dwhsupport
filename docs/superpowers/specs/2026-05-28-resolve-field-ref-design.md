# Design: dialect-level `ResolveFieldRef`

Date: 2026-05-28
Status: Validated, awaiting implementation plan
Supersedes: PR 186 (`quote-user-supplied-column-names`)

## Problem

Monitor SQL on Snowflake tables with case-sensitive or whitespace-bearing column names (e.g. `"Created At"`, `"Price After"`) is generated unquoted, causing the warehouse to reject the query as an invalid identifier:

```sql
count(Price After) as num_not_null  -- SQL compilation error: invalid identifier 'After'
```

The bug lives in `dwhsupport/metrics`: user-supplied field strings flow through `NumericCol`, `TextCol`, and `TimeCol`, whose `ToSql` returns the raw name unmodified.

PR 186 attempted to fix this but introduced two distinct mechanisms doing the same job differently:

1. `metrics/queries.go::resolveFieldRef` uses `dialect.Identifier(...)` which is "always quote" on most dialects (Snowflake `fmt.Sprintf("%q", ...)`, Databricks/BigQuery/ClickHouse `QuoteWithBackticks`). This over-quotes every previously-working column reference and causes 1000+ lines of snapshot churn for cosmetic reasons.
2. `sqldialect/dialect_*.go::ResolveTimeColumn` uses the new fold-aware `QuoteForFoldUpper` / `QuoteForFoldLower` helpers, which leave pure-lower names like `ingested_at` raw on every dialect — the right shape. But only TimeCol gets this treatment.

The fix needs to:
- Apply fold-aware quoting to **all** user-supplied column refs (text, numeric, time), not just time.
- Detect SQL-expression-shaped strings (json paths, casts, function calls) and pass them through raw.
- Preserve current snapshots for the common case (lowercase column names on every dialect).

## Goals

- Snowflake tables with case-sensitive or whitespace column names work for Text/Numeric/Time metric queries.
- Single, consistent quoting mechanism across dialects.
- Snapshot diffs from this change are limited to:
  - Cosmetic strip of redundant quotes on Oracle/MSSQL (currently always-quote).
  - New quoting for mixed-case/whitespace column names on dialects where they were broken.
- No behavioral change for synthetic aliases (`time_segment`, `num_rows`, `field`, metric IDs).

## Non-goals

- Segment expressions (`WithSegment`, `SegmentsListQuery`) routing. Today they go through `Identifier` which always-quotes; not broken. Revisit separately if cosmetic stripping is desired there.
- Per-dialect override of the expression-detection marker set (YAGNI).
- Out-of-tree downstream migrations (kernel-anomalies, synq-monitors, kernel-ext-dwh-metrics) — those repos may need follow-ups if they bypass `TextCol`/`NumericCol`/`TimeCol` and embed raw column names directly.

## Architecture

### New `Dialect.ResolveFieldRef`

Add to the `Dialect` interface in `sqldialect/dialect.go`:

```go
ResolveFieldRef(name string) string
```

Each dialect's implementation follows this shape:

```go
func (d *FooDialect) ResolveFieldRef(name string) string {
    if isLikelyExpression(name) {
        return name
    }
    return <dialect-specific-quoter>(name)
}
```

### Quoting strategies

Dialects split into three families based on their identifier-folding rules.

| Strategy | Dialects | Quoter |
|---|---|---|
| Fold-upper | Snowflake, Oracle | `QuoteForFoldUpper(name, "\"")` |
| Fold-lower | Postgres, Redshift, Trino | `QuoteForFoldLower(name, "\"")` |
| Quote-only-if-needed (backticks) | BigQuery, Databricks, ClickHouse, MySQL | `QuoteWithBackticks(name)` (existing helper, already conditional on `needsQuoting`) |
| Quote-only-if-needed (double quotes) | DuckDB | `QuoteWithDoubleQuotes(name)` (existing helper, already conditional) |
| Quote-only-if-needed (brackets) | MSSQL | new `QuoteWithBracketsIfNeeded(name)` — needs adding because `MSSQLQuoteIdentifier` is unconditional |

`QuoteForFoldUpper` and `QuoteForFoldLower` come from PR 186's `sqldialect/helpers.go` patch — reused unchanged. They:
- Fold-upper: leave pure-upper AND pure-lower bare names raw; quote mixed-case or anything with special chars.
- Fold-lower: leave only pure-lower bare names raw; quote everything else.

`QuoteWithBracketsIfNeeded` (new) — same shape as the existing `QuoteWithBackticks` / `QuoteWithDoubleQuotes`: return raw when `!needsQuoting`, else wrap with `[...]` and escape internal `]` by doubling. MSSQL's `Identifier` keeps using the unconditional `MSSQLQuoteIdentifier` for synthetic aliases (no behavioral change there).

Trino is ANSI fold-lower per spec, so it joins Postgres/Redshift. (PR 186 had it on `d.Identifier`; correct in this design.)

### Expression detection

Shared helper in `sqldialect/helpers.go`:

```go
var sqlExpressionMarkers = []string{
    "(", ")",
    "->>", "->",
    "#>>", "#>",
    "::",
    ",",
    " AS ", " as ",
}

func isLikelyExpression(s string) bool {
    for _, m := range sqlExpressionMarkers {
        if strings.Contains(s, m) {
            return true
        }
    }
    return false
}
```

Heuristic. Known limitations:
- Snowflake VARIANT access `data:field` not covered (`:` not in markers, only `::`). Add later if it surfaces.
- BigQuery struct access `record.field` not covered. Add later if it surfaces.
- Arithmetic on bare columns (`price * 2`) not covered.

YAGNI for now; extend the list when a real case shows up.

### `TextColExpr` / `NumericColExpr` dispatch

Today in `sqldialect/base.go`, both embed `ColBaseExpr{sql: name}` and return `sql` raw via `ColBaseExpr.ToSql`. Change them to override `ToSql`:

```go
func (s *TextColExpr) ToSql(dialect Dialect) (string, error) {
    return dialect.ResolveFieldRef(s.sql), nil
}

func (s *NumericColExpr) ToSql(dialect Dialect) (string, error) {
    return dialect.ResolveFieldRef(s.sql), nil
}
```

`ColBaseExpr` still works for storage; both types just override `ToSql`.

### `TimeColExpr` dispatch (unchanged at caller; refactor inside dialects)

`TimeColExpr.ToSql` already calls `dialect.ResolveTimeColumn`. Keep that.

Refactor each dialect's `ResolveTimeColumn` to delegate to `ResolveFieldRef`:

```go
// BigQuery — only dialect that wraps
func (d *BigQueryDialect) ResolveTimeColumn(expr *TimeColExpr) (string, error) {
    return fmt.Sprintf("timestamp(%s)", d.ResolveFieldRef(expr.name)), nil
}

// All other dialects
func (d *SnowflakeDialect) ResolveTimeColumn(expr *TimeColExpr) (string, error) {
    return d.ResolveFieldRef(expr.name), nil
}
```

### Revert PR 186's metrics-layer changes

The dispatch moves into `sqldialect`, so the following PR 186 changes are no longer needed and revert:
- Delete `metrics/queries.go::resolveFieldRef` and `sqlExpressionMarkers` (logic now in `sqldialect/helpers.go`).
- Drop the new `dialect Dialect` parameter from `TextMetricsValuesCols`, `TextMetricsLengthCols`, `TextMetricsCols`.
- Revert `NumericCol(resolveFieldRef(field, dialect))` back to `NumericCol(field)`.
- Revert `metrics/profile.go` signature changes.
- Revert `metrics/queries_test.go` signature changes.
- Delete `metrics/resolve_field_ref_test.go` (replaced by `sqldialect/resolve_field_ref_test.go`).

Keep PR 186's small refactor in `metrics/queries.go::ApplyMonitorDefArgs` that hoists `partExpr := TimeCol(partitioning.Field)` out of the if/else (clean, unrelated to quoting).

### MSSQL identifier escape fix

PR 186 also fixed `MSSQLQuoteIdentifier` to escape internal `]` characters by doubling. Keep that in scope — one-line fix, unrelated to the main design but a real bug.

### Identifier stays unchanged

`dialect.Identifier(name)` keeps its current always-quote behavior. Used for:
- Synthetic column aliases (`time_segment`, `num_rows`, `field`).
- Fixed metric IDs (`METRIC_NUM_ROWS`, etc.).
- Segment expressions (out of scope for this design).

## Test plan

New `sqldialect/resolve_field_ref_test.go`, table-driven. Cover per dialect family:

| Input | Snowflake / Oracle (fold-upper) | Postgres / Redshift / Trino (fold-lower) | BigQuery / Databricks / ClickHouse / MySQL / DuckDB / MSSQL (quote-if-needed) |
|---|---|---|---|
| `ingested_at` | raw | raw | raw |
| `CREATED_AT` | raw | quoted | raw |
| `createdAt` | quoted | quoted | raw on case-insensitive dialects; quoted on case-sensitive ones — verify per dialect |
| `Created At` | quoted | quoted | quoted |
| `_meta/mtime` | quoted | quoted | quoted (`/` not safe unquoted) |
| `payload->>'amount'` | raw | raw | raw |
| `data->'nested'` | raw | raw | raw |
| `value::numeric` | raw | raw | raw |
| `CAST(x AS INT)` | raw | raw | raw |
| `lower(name)` | raw | raw | raw |
| `coalesce(a, b)` | raw | raw | raw |

Update existing snapshots:
```
UPDATE_SNAPS=true go test ./sqldialect/... ./metrics/... -count=1
```

Manually inspect every snapshot delta. Reject any that doesn't fall into:
- Snowflake/Databricks/etc. lowercase column refs: unchanged.
- Snowflake mixed-case/whitespace column refs: new quoting (bug fix).
- Oracle/MSSQL lowercase column refs: cosmetic quote strip.

Integration tests on Snowflake (if reachable): create a table with `"Created At"` column, run metric queries, confirm execution.

## Breaking changes

- `Dialect` interface gains `ResolveFieldRef(string) string`. Out-of-tree dialect implementations fail to compile until they add the method. Mitigation: this is a private library; downstream impls likely limited to the synq stack.
- `TextColExpr.ToSql` / `NumericColExpr.ToSql` switch from raw passthrough to dialect dispatch. Callers relying on raw passthrough of a SQL fragment (e.g. `TextCol("CAST(x AS TEXT)")`) get correct behavior via `isLikelyExpression` passthrough. Callers passing a bare mixed-case/whitespace name (the broken-today path) get new quoted output.
- Snapshot files across `metrics/` and one `scrapper/databricks/` test change. All changes are cosmetic strip-of-redundant-quotes (Oracle/MSSQL) or correctness fixes (Snowflake mixed-case).

## Migration path

1. Cherry-pick `QuoteForFoldUpper`, `QuoteForFoldLower`, `IsUpper` helpers from PR 186's `sqldialect/helpers.go` into this branch.
2. Add `ResolveFieldRef` to `Dialect` interface + per-dialect implementations.
3. Refactor `TextColExpr.ToSql` / `NumericColExpr.ToSql` to dispatch via `ResolveFieldRef`.
4. Refactor each dialect's `ResolveTimeColumn` to delegate to `ResolveFieldRef`.
5. Revert PR 186's metrics-layer changes (signatures, `resolveFieldRef` helper, test file).
6. Apply MSSQL `]` escape fix from PR 186.
7. Add `sqldialect/resolve_field_ref_test.go`.
8. Regenerate snapshots, inspect diffs.
9. Open new PR. Close PR 186 with pointer to the replacement.

## Files touched

- `sqldialect/dialect.go` — add interface method
- `sqldialect/helpers.go` — add helpers from PR 186 + `isLikelyExpression`
- `sqldialect/dialect_snowflake.go` — `ResolveFieldRef`, `ResolveTimeColumn` refactor
- `sqldialect/dialect_oracle.go` — same
- `sqldialect/dialect_postgres.go` — same
- `sqldialect/dialect_redshift.go` — same
- `sqldialect/dialect_trino.go` — same
- `sqldialect/dialect_bigquery.go` — `ResolveFieldRef`, `ResolveTimeColumn` (keep `timestamp()` wrap)
- `sqldialect/dialect_databricks.go` — same
- `sqldialect/dialect_clickhouse.go` — same
- `sqldialect/dialect_mysql.go` — same
- `sqldialect/dialect_duckdb.go` — same
- `sqldialect/dialect_mssql.go` — same + `]` escape fix
- `sqldialect/base.go` — `TextColExpr.ToSql` / `NumericColExpr.ToSql` override
- `sqldialect/resolve_field_ref_test.go` — new
- `metrics/queries.go` — revert PR 186 signatures + `resolveFieldRef`; keep `partExpr` hoist
- `metrics/profile.go` — revert PR 186 signature changes
- `metrics/queries_test.go` — revert PR 186 signature changes
- `metrics/resolve_field_ref_test.go` — delete (moved to `sqldialect`)
- Snapshot files under `metrics/**/*.snap` and `scrapper/databricks/__snapshots__/*.snap` — regenerate
