package stdsql_test

import (
	"context"
	"database/sql/driver"
	"strings"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/getsynq/dwhsupport/exec"
	"github.com/getsynq/dwhsupport/exec/stdsql"
	"github.com/getsynq/dwhsupport/metrics"
	"github.com/getsynq/dwhsupport/querybuilder"
	"github.com/getsynq/dwhsupport/scrapper"
	dwhsql "github.com/getsynq/dwhsupport/sqldialect"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/suite"
)

// The scan every stdsql-backed warehouse goes through, exercised against the
// result sets a Snowflake account with QUOTED_IDENTIFIERS_IGNORE_CASE = TRUE
// returns.
//
// Such an account stores quoted identifiers upper-cased, so every alias our SQL
// asks for — and all of it is quoted, see SnowflakeDialect.Identifier — comes
// back in a case the query did not ask for. The setting is a migration
// compatibility switch on the customer's own account: nothing on our side can
// turn it off, and forcing it off per session would change the meaning of the
// SQL those customers write in their own tests.
//
// No warehouse is needed to reproduce any of this: the shape of the result set
// is the whole bug.
type HelpersSuite struct {
	suite.Suite
}

func TestHelpersSuite(t *testing.T) {
	suite.Run(t, &HelpersSuite{})
}

// countResult mirrors the struct kernel-anomalies scans a SQL test's count
// query into, tags and all.
type countResult struct {
	Count int64 `ch:"num_failures" bigquery:"num_failures" db:"num_failures"`
}

// resultSet opens a *sqlx.DB over a canned result set, so a scan runs through
// the same driver machinery it does against a warehouse.
func (s *HelpersSuite) resultSet(columns []string, values ...[]driver.Value) *sqlx.DB {
	canned := sqlmock.NewRows(columns)
	for _, value := range values {
		canned = canned.AddRow(value...)
	}
	return s.mockDb(func(mock sqlmock.Sqlmock) {
		mock.ExpectQuery(".*").WillReturnRows(canned)
	})
}

func (s *HelpersSuite) mockDb(expect func(sqlmock.Sqlmock)) *sqlx.DB {
	db, mock, err := sqlmock.New()
	s.Require().NoError(err)
	s.T().Cleanup(func() { _ = db.Close() })
	expect(mock)
	return sqlx.NewDb(db, "sqlmock")
}

// upperCased is what the driver hands back for the aliases below on an account
// that folds quoted identifiers.
func upperCased(columns ...string) []string {
	out := make([]string, len(columns))
	for i, column := range columns {
		out[i] = strings.ToUpper(column)
	}
	return out
}

//
// Path A — QueryMany, which fails loudly: sqlx's StructScan rejects every row
// over a single column no `db` tag claims.
//

// The SQL test count query. `AS "num_failures"` comes back as NUM_FAILURES, and
// the scan dies with `missing destination name NUM_FAILURES in *countResult`, so
// every SQL test on the account errors instead of running.
func (s *HelpersSuite) TestSqlTestCountScansWhenTheAccountUppercasesQuotedAliases() {
	db := s.resultSet(upperCased("num_failures"), []driver.Value{int64(7)})

	got, err := stdsql.QueryMany[countResult](context.Background(), db, `select count(*) as "num_failures" from t`)

	s.Require().NoError(err)
	s.Require().Len(got, 1)
	s.EqualValues(7, got[0].Count)
}

// The volume monitor. Its query is built by metrics/querybuilder, so the aliases
// under test here are the ones production actually asks for rather than a
// transcription of them.
func (s *HelpersSuite) TestVolumeMonitorScansWhenTheAccountUppercasesQuotedAliases() {
	sql := s.volumeMonitorSql()
	s.Contains(sql, `as "segment"`)
	s.Contains(sql, `as "num_rows"`)

	segmentedAt := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	db := s.resultSet(
		upperCased("segment", "time_segment", "num_rows"),
		[]driver.Value{"eu", segmentedAt, int64(1234)},
	)

	got, err := stdsql.QueryMany[metrics.MetricVolume](context.Background(), db, sql)

	s.Require().NoError(err)
	s.Require().Len(got, 1)
	s.Equal("eu", got[0].Segment)
	s.Equal(segmentedAt, got[0].TimeSegment.UTC())
	s.EqualValues(1234, got[0].NumRows)
}

// The numeric field profile, the widest of the monitor result shapes. Its query
// carries no segmentation, so `segment` and `time_segment` are absent from the
// result set and have to stay at their zero values rather than fail the scan.
func (s *HelpersSuite) TestNumericProfileScansWhenTheAccountUppercasesQuotedAliases() {
	sql := s.numericProfileSql()

	db := s.resultSet(
		upperCased("field", "num_rows", "num_not_null", "num_unique", "num_empty", "mean", "min", "max", "median", "stddev"),
		[]driver.Value{"amount", int64(10), int64(9), int64(8), int64(1), 2.5, 0.0, 5.0, 2.0, 1.5},
	)

	got, err := stdsql.QueryMany[metrics.MetricNumericFieldStats](context.Background(), db, sql)

	s.Require().NoError(err)
	s.Require().Len(got, 1)
	s.Equal("amount", got[0].Field)
	s.EqualValues(10, got[0].NumTotal)
	s.Require().NotNil(got[0].NumUnique)
	s.EqualValues(8, *got[0].NumUnique)
	s.Require().NotNil(got[0].Median)
	s.EqualValues(2.0, *got[0].Median)
	s.Equal("", got[0].Segment)
	s.True(got[0].TimeSegment.IsZero())
}

// The catalog read of every other stdsql warehouse goes through the same helper,
// so it gains the same tolerance.
func (s *HelpersSuite) TestCatalogRowScansWhenTheAccountUppercasesQuotedAliases() {
	updatedAt := time.Date(2026, 8, 26, 13, 29, 0, 0, time.UTC)
	db := s.resultSet(
		upperCased("database", "schema", "table", "row_count", "updated_at", "size_bytes"),
		[]driver.Value{"ANALYTICS", "PUBLIC", "ORDERS", int64(42), updatedAt, int64(4096)},
	)

	got, err := stdsql.QueryMany[scrapper.TableMetricsRow](context.Background(), db, "select ...")

	s.Require().NoError(err)
	s.Require().Len(got, 1)
	s.Equal("ORDERS", got[0].Table)
	s.Require().NotNil(got[0].RowCount)
	s.EqualValues(42, *got[0].RowCount)
}

// The ordinary account, where the aliases come back exactly as asked for.
// Matching either case has to mean matching this one too.
func (s *HelpersSuite) TestScansWhenTheAccountKeepsQuotedAliasesAsWritten() {
	db := s.resultSet([]string{"num_failures"}, []driver.Value{int64(3)})

	got, err := stdsql.QueryMany[countResult](context.Background(), db, `select count(*) as "num_failures" from t`)

	s.Require().NoError(err)
	s.Require().Len(got, 1)
	s.EqualValues(3, got[0].Count)
}

// Nothing says a warehouse folds to one case or the other — MySQL hands back
// whatever case the alias was written in, and a proxy may rewrite it.
func (s *HelpersSuite) TestScansWhenTheAliasComesBackInMixedCase() {
	db := s.resultSet([]string{"Num_Failures"}, []driver.Value{int64(5)})

	got, err := stdsql.QueryMany[countResult](context.Background(), db, `select count(*) as "num_failures" from t`)

	s.Require().NoError(err)
	s.Require().Len(got, 1)
	s.EqualValues(5, got[0].Count)
}

// A warehouse-managed view gains a column on a vendor release without notice.
// Failing every row over it turns an ingest into a total outage, so an unclaimed
// column is discarded and the rest of the row is kept.
func (s *HelpersSuite) TestAColumnNoFieldClaimsIsDiscarded() {
	db := s.resultSet(
		[]string{"segment", "time_segment", "num_rows", "spcs_job_id"},
		[]driver.Value{"eu", time.Now().UTC(), int64(9), "a-job-id"},
	)

	got, err := stdsql.QueryMany[metrics.MetricVolume](context.Background(), db, "select ...")

	s.Require().NoError(err)
	s.Require().Len(got, 1)
	s.EqualValues(9, got[0].NumRows)
}

// The other direction: a column withdrawn, or a read pointed at a view exposing
// a subset. The field it fed keeps its zero value.
func (s *HelpersSuite) TestAFieldNoColumnFeedsKeepsItsZeroValue() {
	db := s.resultSet([]string{"NUM_ROWS"}, []driver.Value{int64(9)})

	got, err := stdsql.QueryMany[metrics.MetricVolume](context.Background(), db, "select ...")

	s.Require().NoError(err)
	s.Require().Len(got, 1)
	s.EqualValues(9, got[0].NumRows)
	s.Equal("", got[0].Segment)
}

// A duplicated column name would otherwise have its later copy silently
// overwrite the earlier one.
func (s *HelpersSuite) TestADuplicatedColumnDoesNotOverwriteTheFirst() {
	db := s.resultSet(
		[]string{"NUM_ROWS", "num_rows"},
		[]driver.Value{int64(9), int64(11)},
	)

	got, err := stdsql.QueryMany[metrics.MetricVolume](context.Background(), db, "select ...")

	s.Require().NoError(err)
	s.Require().Len(got, 1)
	s.EqualValues(9, got[0].NumRows)
}

// NULL has to keep reaching a pointer field rather than the scan rejecting it.
func (s *HelpersSuite) TestANullReachesAPointerField() {
	db := s.resultSet(
		upperCased("database", "schema", "table", "row_count", "updated_at", "size_bytes"),
		[]driver.Value{"ANALYTICS", "PUBLIC", "ORDERS", nil, nil, nil},
	)

	got, err := stdsql.QueryMany[scrapper.TableMetricsRow](context.Background(), db, "select ...")

	s.Require().NoError(err)
	s.Require().Len(got, 1)
	s.Nil(got[0].RowCount)
	s.Nil(got[0].UpdatedAt)
	s.Nil(got[0].SizeBytes)
}

// Post-processors are how a scrapper fills in what the warehouse does not carry,
// so they have to keep running over the scanned row.
func (s *HelpersSuite) TestPostProcessorsRunOverEveryScannedRow() {
	db := s.resultSet(
		upperCased("database", "schema", "table"),
		[]driver.Value{"ANALYTICS", "PUBLIC", "ORDERS"},
		[]driver.Value{"ANALYTICS", "PUBLIC", "CUSTOMERS"},
	)

	got, err := stdsql.QueryMany(context.Background(), db, "select ...",
		exec.WithPostProcessors(func(row *scrapper.TableMetricsRow) (*scrapper.TableMetricsRow, error) {
			row.Instance = "an-account"
			return row, nil
		}),
	)

	s.Require().NoError(err)
	s.Require().Len(got, 2)
	s.Equal("an-account", got[0].Instance)
	s.Equal("an-account", got[1].Instance)
}

// A post-processor returning nil drops the row — that is how blocklists filter.
func (s *HelpersSuite) TestAPostProcessorReturningNilDropsTheRow() {
	db := s.resultSet(
		upperCased("database", "schema", "table"),
		[]driver.Value{"ANALYTICS", "PUBLIC", "ORDERS"},
		[]driver.Value{"ANALYTICS", "INTERNAL", "CUSTOMERS"},
	)

	got, err := stdsql.QueryMany(context.Background(), db, "select ...",
		exec.WithPostProcessors(func(row *scrapper.TableMetricsRow) (*scrapper.TableMetricsRow, error) {
			if row.Schema == "INTERNAL" {
				return nil, nil
			}
			return row, nil
		}),
	)

	s.Require().NoError(err)
	s.Require().Len(got, 1)
	s.Equal("ORDERS", got[0].Table)
}

func (s *HelpersSuite) TestAPostProcessorErrorIsReturned() {
	db := s.resultSet(upperCased("num_failures"), []driver.Value{int64(7)})

	_, err := stdsql.QueryMany(context.Background(), db, "select ...",
		exec.WithPostProcessors(func(row *countResult) (*countResult, error) {
			return nil, errors.New("boom")
		}),
	)

	s.Require().Error(err)
	s.Contains(err.Error(), "boom")
}

func (s *HelpersSuite) TestArgsReachTheDriver() {
	db := s.mockDb(func(mock sqlmock.Sqlmock) {
		mock.ExpectQuery(".*").
			WithArgs("PUBLIC").
			WillReturnRows(sqlmock.NewRows(upperCased("num_failures")).AddRow(int64(2)))
	})

	got, err := stdsql.QueryMany[countResult](context.Background(), db, "select ... where schema = ?",
		exec.WithArgs[countResult]("PUBLIC"),
	)

	s.Require().NoError(err)
	s.Require().Len(got, 1)
	s.EqualValues(2, got[0].Count)
}

func (s *HelpersSuite) TestAnEmptyResultSetIsNotAnError() {
	db := s.resultSet(upperCased("num_failures"))

	got, err := stdsql.QueryMany[countResult](context.Background(), db, "select ...")

	s.Require().NoError(err)
	s.Empty(got)
}

// A querier is prepared with whatever type the caller names, including a scalar
// one that never scans — dwhconnect.PrepareQuerier[int64] is used to open a
// connection and nothing else. Planning a scan it never performs must not turn
// that into an error.
func (s *HelpersSuite) TestAnEmptyResultSetIsNotAnErrorForANonStructTarget() {
	db := s.resultSet([]string{"one"})

	got, err := stdsql.QueryMany[int64](context.Background(), db, "select 1 as one")

	s.Require().NoError(err)
	s.Empty(got)
}

func (s *HelpersSuite) TestAQueryErrorIsReturned() {
	db := s.mockDb(func(mock sqlmock.Sqlmock) {
		mock.ExpectQuery(".*").WillReturnError(errors.New("syntax error at or near"))
	})

	_, err := stdsql.QueryMany[countResult](context.Background(), db, "select ...")

	s.Require().Error(err)
	s.Contains(err.Error(), "syntax error")
}

func (s *HelpersSuite) TestARowErrorIsReturned() {
	db := s.mockDb(func(mock sqlmock.Sqlmock) {
		mock.ExpectQuery(".*").WillReturnRows(
			sqlmock.NewRows(upperCased("num_failures")).
				AddRow(int64(1)).
				AddRow(int64(2)).
				RowError(1, errors.New("connection reset")),
		)
	})

	_, err := stdsql.QueryMany[countResult](context.Background(), db, "select ...")

	s.Require().Error(err)
	s.Contains(err.Error(), "connection reset")
}

//
// QueryAndProcessMany — the batched form, used by the catalog ingests.
//

func (s *HelpersSuite) TestBatchedScanWhenTheAccountUppercasesQuotedAliases() {
	rows := sqlmock.NewRows(upperCased("database", "schema", "table"))
	for _, table := range []string{"ORDERS", "CUSTOMERS", "PAYMENTS", "REFUNDS", "SHIPMENTS"} {
		rows = rows.AddRow("ANALYTICS", "PUBLIC", table)
	}
	db := s.mockDb(func(mock sqlmock.Sqlmock) { mock.ExpectQuery(".*").WillReturnRows(rows) })

	var batches [][]string
	err := stdsql.QueryAndProcessMany(context.Background(), db, "select ...",
		func(ctx context.Context, rows []*scrapper.TableMetricsRow) error {
			batch := make([]string, 0, len(rows))
			for _, row := range rows {
				batch = append(batch, row.Table)
			}
			batches = append(batches, batch)
			return nil
		},
		exec.WithProcessBatchSize[scrapper.TableMetricsRow](2),
	)

	s.Require().NoError(err)
	s.Equal([][]string{
		{"ORDERS", "CUSTOMERS"},
		{"PAYMENTS", "REFUNDS"},
		{"SHIPMENTS"},
	}, batches)
}

func (s *HelpersSuite) TestABatchHandlerErrorStopsTheRead() {
	rows := sqlmock.NewRows(upperCased("database", "schema", "table"))
	for _, table := range []string{"ORDERS", "CUSTOMERS", "PAYMENTS", "REFUNDS"} {
		rows = rows.AddRow("ANALYTICS", "PUBLIC", table)
	}
	db := s.mockDb(func(mock sqlmock.Sqlmock) { mock.ExpectQuery(".*").WillReturnRows(rows) })

	var calls int
	err := stdsql.QueryAndProcessMany(context.Background(), db, "select ...",
		func(ctx context.Context, rows []*scrapper.TableMetricsRow) error {
			calls++
			return errors.New("downstream write failed")
		},
		exec.WithProcessBatchSize[scrapper.TableMetricsRow](2),
	)

	s.Require().Error(err)
	s.Contains(err.Error(), "downstream write failed")
	s.Equal(1, calls)
}

func (s *HelpersSuite) TestBatchedPostProcessorsRunAndCanDropRows() {
	rows := sqlmock.NewRows(upperCased("database", "schema", "table"))
	for _, schema := range []string{"PUBLIC", "INTERNAL", "PUBLIC"} {
		rows = rows.AddRow("ANALYTICS", schema, "ORDERS")
	}
	db := s.mockDb(func(mock sqlmock.Sqlmock) { mock.ExpectQuery(".*").WillReturnRows(rows) })

	var seen int
	err := stdsql.QueryAndProcessMany(context.Background(), db, "select ...",
		func(ctx context.Context, rows []*scrapper.TableMetricsRow) error {
			seen += len(rows)
			for _, row := range rows {
				s.Equal("an-account", row.Instance)
			}
			return nil
		},
		exec.WithPostProcessors(func(row *scrapper.TableMetricsRow) (*scrapper.TableMetricsRow, error) {
			if row.Schema == "INTERNAL" {
				return nil, nil
			}
			row.Instance = "an-account"
			return row, nil
		}),
	)

	s.Require().NoError(err)
	s.Equal(2, seen)
}

//
// Path B — QueryMaps, which is the silent one. The map is keyed by the driver's
// column names, and every caller indexes it with the alias its own SQL asked
// for. A miss reads as zero, so a SQL test that should have caught bad data
// reports green and nothing raises an error.
//

// lookup is how a caller reads one column out of a QueryMaps row. The body is
// the whole of Path B: every caller in kernel-anomalies indexes the map with the
// alias its own SQL asked for.
func lookup(row exec.QueryMapResult, alias string) (any, bool) {
	return row.Get(alias)
}

// readInt64 mirrors how kernel-anomalies reads an evaluator's counts out of a
// QueryMaps row: a miss reads as zero and raises nothing.
func readInt64(row exec.QueryMapResult, alias string) int64 {
	v, ok := lookup(row, alias)
	if !ok {
		return 0
	}
	n, _ := v.(int64)
	return n
}

func (s *HelpersSuite) TestEvaluatorCountsAreReadWhenTheAccountUppercasesQuotedAliases() {
	db := s.resultSet(
		upperCased("fail_0", "fail_1", "fail_any"),
		[]driver.Value{int64(3), int64(0), int64(3)},
	)

	got, err := stdsql.QueryMaps(context.Background(), db, `select ... as "fail_0", ... as "fail_any"`)

	s.Require().NoError(err)
	s.Require().Len(got, 1)
	s.EqualValues(3, readInt64(got[0], "fail_0"), "an evaluator that failed 3 rows must not report zero")
	s.EqualValues(0, readInt64(got[0], "fail_1"))
	s.EqualValues(3, readInt64(got[0], "fail_any"))
}

func (s *HelpersSuite) TestEvaluatorCountsAreReadWhenTheAccountKeepsAliasesAsWritten() {
	db := s.resultSet([]string{"fail_0", "fail_any"}, []driver.Value{int64(2), int64(2)})

	got, err := stdsql.QueryMaps(context.Background(), db, "select ...")

	s.Require().NoError(err)
	s.Require().Len(got, 1)
	s.EqualValues(2, readInt64(got[0], "fail_0"))
}

// A column that genuinely is not there still has to read as absent, or the fold
// would paper over an alias we got wrong.
func (s *HelpersSuite) TestAnAbsentColumnIsStillAbsent() {
	db := s.resultSet(upperCased("fail_0"), []driver.Value{int64(3)})

	got, err := stdsql.QueryMaps(context.Background(), db, "select ...")

	s.Require().NoError(err)
	s.Require().Len(got, 1)
	_, found := lookup(got[0], "fail_1")
	s.False(found)
}

// Present but NULL is not the same as absent: the evaluator ran and produced
// nothing rather than never having been asked for.
func (s *HelpersSuite) TestAColumnPresentAndNullIsFound() {
	db := s.resultSet(upperCased("fail_0"), []driver.Value{nil})

	got, err := stdsql.QueryMaps(context.Background(), db, "select ...")

	s.Require().NoError(err)
	s.Require().Len(got, 1)
	value, found := lookup(got[0], "fail_0")
	s.True(found)
	s.Nil(value)
}

// The keys stay as the warehouse spelled them: the audit rows a SQL test shows
// are built by ranging over this map, and a customer's own column names are what
// they expect to see there.
func (s *HelpersSuite) TestTheKeysStayAsTheWarehouseSpelledThem() {
	db := s.resultSet([]string{"ORDER_ID", "customer_name"}, []driver.Value{int64(1), "acme"})

	got, err := stdsql.QueryMaps(context.Background(), db, "select ...")

	s.Require().NoError(err)
	s.Require().Len(got, 1)
	s.ElementsMatch([]string{"ORDER_ID", "customer_name"}, keysOf(got[0]))
}

func keysOf(row exec.QueryMapResult) []string {
	keys := make([]string, 0, len(row))
	for key := range row {
		keys = append(keys, key)
	}
	return keys
}

//
// The queries under test, built the way production builds them.
//

func (s *HelpersSuite) volumeMonitorSql() string {
	dialect := dwhsql.NewSnowflakeDialect()
	builder := querybuilder.
		NewQueryBuilder(dwhsql.TableFqn("ANALYTICS", "PUBLIC", "ORDERS"), metrics.TableVolumeMetricsCols()).
		WithSegment(dwhsql.Identifier("REGION"))
	sql, err := builder.ToSql(dialect)
	s.Require().NoError(err)
	return sql
}

func (s *HelpersSuite) numericProfileSql() string {
	dialect := dwhsql.NewSnowflakeDialect()
	builder := querybuilder.NewQueryBuilder(
		dwhsql.TableFqn("ANALYTICS", "PUBLIC", "ORDERS"),
		metrics.NumericMetricsCols("AMOUNT", dialect),
	)
	sql, err := builder.ToSql(dialect)
	s.Require().NoError(err)
	return sql
}
