package scrappertest

import (
	"context"
	"errors"
	"io"
	"strings"

	"github.com/getsynq/dwhsupport/exec/querycontext"
	"github.com/getsynq/dwhsupport/scrapper"
	. "github.com/getsynq/dwhsupport/sqldialect"
	"github.com/stretchr/testify/suite"
)

// SqlDialectExecutionConfig points the SqlDialectExecutionSuite at a table it
// can read. The suite never writes, and the numbers it asserts on are derived
// from the table itself, so any table with these three columns works.
type SqlDialectExecutionConfig struct {
	// TableFqn is the fully qualified name of the table to read.
	TableFqn *TableFqnExpr

	// KeyField is a sortable, preferably unique numeric column (e.g. "id").
	// It is what the window functions order by.
	KeyField string

	// SegmentField is a low-cardinality categorical column (e.g. "category"),
	// used as the PARTITION BY key.
	SegmentField string
}

// SqlDialectExecutionSuite validates that SQL built by the sqldialect builders
// parses and executes on a real warehouse, and that a value comes back under
// the alias the builder asked for.
//
// Snapshot tests pin what the builders emit; they cannot tell whether the
// engine accepts it. The shapes covered here are the ones reconciliation
// depends on — a side query wrapped as a derived table, NTILE/ROW_NUMBER
// checkpoints over that derived table, and columns referenced through the
// derived table's alias — each of which has a dialect that rejects the obvious
// rendering.
//
// Embed this in warehouse-specific integration test suites. Set Scrapper and
// Config before tests run (e.g. in SetupSuite).
type SqlDialectExecutionSuite struct {
	suite.Suite
	Scrapper scrapper.Scrapper
	Config   SqlDialectExecutionConfig
}

// Ctx returns a background context for use in SetupSuite of embedding suites.
func (s *SqlDialectExecutionSuite) Ctx() context.Context {
	return context.Background()
}

func (s *SqlDialectExecutionSuite) ctx() context.Context {
	return querycontext.WithQueryContext(context.Background(), complianceQueryContext)
}

func (s *SqlDialectExecutionSuite) skipIfNil() {
	if s.Scrapper == nil {
		s.T().Skip("Scrapper not set")
	}
	if s.Config.TableFqn == nil {
		s.T().Skip("TableFqn not configured")
	}
	if s.Config.KeyField == "" {
		s.T().Skip("KeyField not configured")
	}
}

// sideQuery is the raw SQL a caller wraps as a derived table — the shape
// reconciliation starts from, where the query is supplied by the user and
// dwhsupport only gets to wrap it.
func (s *SqlDialectExecutionSuite) sideQuery() string {
	s.T().Helper()
	fqnSql, err := s.Config.TableFqn.ToSql(s.Scrapper.SqlDialect())
	s.Require().NoError(err)
	return "select * from " + fqnSql
}

// execute renders the select, runs it, and returns the rows keyed by the
// column names the driver reported. Reading a value back out goes through
// column(), which folds case the way exec.QueryMapResult.Get does — a
// Snowflake account with QUOTED_IDENTIFIERS_IGNORE_CASE spells our aliases
// back upper-cased.
func (s *SqlDialectExecutionSuite) execute(sel *Select) []map[string]*scrapper.ColumnValue {
	s.T().Helper()

	sql, err := sel.ToSql(s.Scrapper.SqlDialect())
	s.Require().NoError(err, "SQL generation should not error")
	s.T().Logf("Generated SQL:\n%s", sql)

	it, err := s.Scrapper.RunRawQuery(s.ctx(), sql)
	if errors.Is(err, scrapper.ErrUnsupported) {
		s.T().Skip("RunRawQuery unsupported")
	}
	s.Require().NoError(err, "query should execute")
	defer it.Close()

	var rows []map[string]*scrapper.ColumnValue
	for {
		row, err := it.Next(s.ctx())
		if errors.Is(err, io.EOF) {
			break
		}
		s.Require().NoError(err)

		byName := map[string]*scrapper.ColumnValue{}
		for _, cv := range row {
			byName[cv.Name] = cv
		}
		rows = append(rows, byName)
	}
	return rows
}

// column reads one column out of a row by the alias the query asked for,
// matching case-insensitively.
func (s *SqlDialectExecutionSuite) column(row map[string]*scrapper.ColumnValue, alias string) *scrapper.ColumnValue {
	s.T().Helper()
	for name, cv := range row {
		if strings.EqualFold(name, alias) {
			return cv
		}
	}
	s.Failf("column not found", "no column %q in %v", alias, keysOf(row))
	return nil
}

func keysOf(row map[string]*scrapper.ColumnValue) []string {
	names := make([]string, 0, len(row))
	for name := range row {
		names = append(names, name)
	}
	return names
}

// TestSqlDialectExecution_SubqueryTable wraps a side query as a derived table
// and counts it. Oracle rejects the `AS` keyword before a derived table's
// alias (ORA-03048), which failed every reconciliation mode before the first
// row was read.
func (s *SqlDialectExecutionSuite) TestSqlDialectExecution_SubqueryTable() {
	s.skipIfNil()

	sel := NewSelect().
		From(SubqueryTable(s.sideQuery(), "_recon_base")).
		Cols(As(CountAll(), Alias("row_count")))

	rows := s.execute(sel)
	s.Require().Len(rows, 1)
	s.False(s.column(rows[0], "row_count").IsNull, "row_count should not be null")
}

// TestSqlDialectExecution_CteWithLimit is the data-preview shape: an arbitrary
// query wrapped in a CTE whose name starts with an underscore, read back and
// capped. Oracle (ORA-00911) and Db2 (SQL20521N, a leading `_` starts a
// conditional compilation directive) reject that name unquoted, so the CTE's
// declaration, the FROM that reads it and any column qualified by it all have
// to agree on quoting it.
func (s *SqlDialectExecutionSuite) TestSqlDialectExecution_CteWithLimit() {
	s.skipIfNil()

	const cteName = "_synq_preview_cte"
	cte := CteFqn(cteName)
	sel := NewSelect().
		Cte(cte, Sql(s.sideQuery())).
		From(cte).
		Cols(As(QualifiedCol(cteName, s.Config.KeyField), Alias("key_val"))).
		WithLimit(Limit(Int64(5)))

	rows := s.execute(sel)
	s.Require().NotEmpty(rows)
	s.LessOrEqual(len(rows), 5)
	s.False(s.column(rows[0], "key_val").IsNull)
}

// TestSqlDialectExecution_QualifiedCol references a column through the derived
// table's alias. The alias and the qualifier have to be spelled the same way
// after the dialect's case folding, which is why both go through the builder
// rather than being formatted into the string.
func (s *SqlDialectExecutionSuite) TestSqlDialectExecution_QualifiedCol() {
	s.skipIfNil()

	sel := NewSelect().
		From(SubqueryTable(s.sideQuery(), "_recon_base")).
		Cols(
			As(Fn("MIN", QualifiedCol("_recon_base", s.Config.KeyField)), Alias("min_key")),
			As(Fn("MAX", QualifiedCol("_recon_base", s.Config.KeyField)), Alias("max_key")),
		)

	rows := s.execute(sel)
	s.Require().Len(rows, 1)
	s.False(s.column(rows[0], "min_key").IsNull, "min_key should not be null")
	s.False(s.column(rows[0], "max_key").IsNull, "max_key should not be null")
}

// TestSqlDialectExecution_RowNumber runs ROW_NUMBER() OVER (PARTITION BY ...
// ORDER BY ...) against the derived table and reads the first row of each
// partition.
func (s *SqlDialectExecutionSuite) TestSqlDialectExecution_RowNumber() {
	s.skipIfNil()
	if s.Config.SegmentField == "" {
		s.T().Skip("SegmentField not configured")
	}

	numbered := NewSelect().
		From(SubqueryTable(s.sideQuery(), "_recon_base")).
		Cols(
			As(TextCol(s.Config.KeyField), Alias("key_val")),
			As(
				Over(RowNumber()).
					PartitionBy(TextCol(s.Config.SegmentField)).
					OrderBy(Asc(TextCol(s.Config.KeyField))),
				Alias("rn"),
			),
		)

	numberedSql, err := numbered.ToSql(s.Scrapper.SqlDialect())
	s.Require().NoError(err)

	sel := NewSelect().
		From(SubqueryTable(numberedSql, "_recon_numbered")).
		Cols(As(QualifiedCol("_recon_numbered", "key_val"), Alias("key_val"))).
		Where(Eq(QualifiedCol("_recon_numbered", "rn"), Int64(1))).
		OrderBy(Asc(QualifiedCol("_recon_numbered", "key_val")))

	rows := s.execute(sel)
	s.Require().NotEmpty(rows, "each partition should contribute a first row")
	s.False(s.column(rows[0], "key_val").IsNull)
}

// TestSqlDialectExecution_NtileCheckpoints is the reconciliation checkpoint
// query: bucket the key space with NTILE, then take each bucket's bounds.
// Oracle and MSSQL require the ORDER BY inside the OVER clause for NTILE, and
// none of the dialects accept the window function in a GROUP BY, so the
// bucketing has to happen one level down.
func (s *SqlDialectExecutionSuite) TestSqlDialectExecution_NtileCheckpoints() {
	s.skipIfNil()

	const buckets = 4

	bucketed := NewSelect().
		From(SubqueryTable(s.sideQuery(), "_recon_base")).
		Cols(
			As(TextCol(s.Config.KeyField), Alias("key_val")),
			As(
				Over(Ntile(Int64(buckets))).OrderBy(Asc(TextCol(s.Config.KeyField))),
				Alias("bucket"),
			),
		)

	bucketedSql, err := bucketed.ToSql(s.Scrapper.SqlDialect())
	s.Require().NoError(err)

	sel := NewSelect().
		From(SubqueryTable(bucketedSql, "_recon_bucketed")).
		Cols(
			As(QualifiedCol("_recon_bucketed", "bucket"), Alias("bucket")),
			As(Fn("MIN", QualifiedCol("_recon_bucketed", "key_val")), Alias("lo")),
			As(Fn("MAX", QualifiedCol("_recon_bucketed", "key_val")), Alias("hi")),
			As(CountAll(), Alias("cnt")),
		).
		GroupBy(QualifiedCol("_recon_bucketed", "bucket")).
		OrderBy(Asc(QualifiedCol("_recon_bucketed", "bucket")))

	rows := s.execute(sel)
	s.Require().NotEmpty(rows, "NTILE should produce at least one bucket")
	s.LessOrEqual(len(rows), buckets, "NTILE should not produce more buckets than asked for")

	for _, row := range rows {
		s.False(s.column(row, "bucket").IsNull)
		s.False(s.column(row, "lo").IsNull)
		s.False(s.column(row, "hi").IsNull)
		s.False(s.column(row, "cnt").IsNull)
	}
}

// identConformanceNames are the identifier shapes a customer actually manages
// to write. They are exercised as column aliases rather than as objects, so
// the suite still never writes — an alias is an identifier, and the engine
// reports back what it made of it.
//
// The delimiter characters are deliberately absent. BigQuery parses
// `select 1 as ` + "`we\\`ird`" + ` exactly as intended and then rejects the result with
// "Invalid field name", because what a column may be *named* is a separate
// rule from how an identifier is quoted — so a failure here would say nothing
// about the escape. The escapes are pinned by the per-dialect goldens and by
// the Ident round-trip test instead.
var identConformanceNames = []string{
	"order",
	"group",
	"orders",
	"ORDERS",
	"MyTable",
	"Created At",
	"my-table",
	"zamówienia",
}

// TestSqlDialectExecution_IdentQuotingIsAccepted renders each awkward name as
// a quoted alias and reads the value back under it.
//
// This is what the snapshots cannot tell us: whether the engine accepts the
// delimiters we chose for it, and whether a reserved word survives being
// quoted — the failure this whole concept exists to prevent.
func (s *SqlDialectExecutionSuite) TestSqlDialectExecution_IdentQuotingIsAccepted() {
	s.skipIfNil()

	dialect := s.Scrapper.SqlDialect()
	for _, name := range identConformanceNames {
		s.Run(name, func() {
			ident := CanonicalIdent(name)
			sel := NewSelect().
				From(SubqueryTable(s.sideQuery(), "_recon_base")).
				Cols(As(Int64(1), ident)).
				WithLimit(Limit(Int64(1)))

			rows := s.execute(sel)
			s.Require().Len(rows, 1)
			s.False(s.column(rows[0], ident.Name(dialect)).IsNull)
		})
	}
}

// TestSqlDialectExecution_WrittenIdentBehavesLikeTheUnquotedReference is the
// invariant that makes the conversion safe to ship, and it is asserted the
// only way that holds on every engine: run the same aggregate twice, once with
// the name written straight into the SQL and once through WrittenIdent, and
// require the two to agree — including when they agree by both failing.
//
// Whatever the engine does with the unquoted form, the quoted form has to do
// the same thing. On Snowflake a lower-case name folds up and both resolve; on
// ClickHouse an upper-case one resolves on neither side because the engine is
// case-sensitive; on Trino both fold down. A fold that is wrong in either
// direction breaks the agreement, on the engine rather than in a snapshot.
//
// A column alias is deliberately not used as the probe. Trino and Athena
// report an alias back under the case it was written in even though they
// resolve an object name folded to lower, so what an alias comes back as says
// nothing about how a reference resolves.
func (s *SqlDialectExecutionSuite) TestSqlDialectExecution_WrittenIdentBehavesLikeTheUnquotedReference() {
	s.skipIfNil()

	for _, written := range []string{
		s.Config.KeyField,
		strings.ToLower(s.Config.KeyField),
		strings.ToUpper(s.Config.KeyField),
	} {
		s.Run(written, func() {
			unquoted, unquotedErr := s.tryScalar(Fn("MIN", Sql(written)))
			quoted, quotedErr := s.tryScalar(Fn("MIN", WrittenIdent(written)))

			if unquotedErr != nil {
				s.Errorf(quotedErr, "unquoted %q was rejected (%v) but WrittenIdent resolved it", written, unquotedErr)
				return
			}
			s.Require().NoError(quotedErr, "unquoted %q resolves, so WrittenIdent must too", written)
			s.Equal(unquoted, quoted, "WrittenIdent(%q) reached a different column than the unquoted reference", written)
		})
	}
}

// tryScalar runs one aggregate over the side query and returns its value,
// handing back the engine's error instead of failing the test — the callers
// here are asking what the engine does, not asserting that it succeeds.
func (s *SqlDialectExecutionSuite) tryScalar(expr Expr) (any, error) {
	s.T().Helper()

	sel := NewSelect().
		From(SubqueryTable(s.sideQuery(), "_recon_base")).
		Cols(As(expr, Alias("probe")))

	sql, err := sel.ToSql(s.Scrapper.SqlDialect())
	s.Require().NoError(err)
	s.T().Logf("Generated SQL:\n%s", sql)

	it, err := s.Scrapper.RunRawQuery(s.ctx(), sql)
	if errors.Is(err, scrapper.ErrUnsupported) {
		s.T().Skip("RunRawQuery unsupported")
	}
	if err != nil {
		return nil, err
	}
	defer it.Close()

	row, err := it.Next(s.ctx())
	if err != nil {
		return nil, err
	}
	for _, cv := range row {
		if strings.EqualFold(cv.Name, "probe") {
			return cv.Value, nil
		}
	}
	s.Failf("column not found", "no probe column in %v", row)
	return nil, nil
}

// TestSqlDialectExecution_QualifiedIdentNamesTheTable reads the configured
// table through QualifiedIdent instead of through ResolveFqn, which is how a
// table reference from a config file reaches the FROM clause.
func (s *SqlDialectExecutionSuite) TestSqlDialectExecution_QualifiedIdentNamesTheTable() {
	s.skipIfNil()

	fqn := QualifiedIdent(
		WrittenIdent(s.Config.TableFqn.ProjectId()),
		WrittenIdent(s.Config.TableFqn.DatasetId()),
		WrittenIdent(s.Config.TableFqn.TableId()),
	)
	if !s.Scrapper.SqlDialect().SupportsCrossDatabaseQueries() {
		fqn = QualifiedIdent(
			WrittenIdent(s.Config.TableFqn.DatasetId()),
			WrittenIdent(s.Config.TableFqn.TableId()),
		)
	}

	sel := NewSelect().From(fqn).Cols(As(CountAll(), Alias("row_count")))

	rows := s.execute(sel)
	s.Require().Len(rows, 1)
	s.False(s.column(rows[0], "row_count").IsNull)
}
