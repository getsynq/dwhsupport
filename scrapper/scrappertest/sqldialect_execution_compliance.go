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
