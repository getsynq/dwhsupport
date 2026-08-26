package snowflake

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/getsynq/dwhsupport/exec"
	"github.com/getsynq/dwhsupport/metrics"
	"github.com/getsynq/dwhsupport/querybuilder"
	dwhsql "github.com/getsynq/dwhsupport/sqldialect"
	"github.com/stretchr/testify/suite"
)

// QuotedIdentifiersIgnoreCaseSuite runs the monitor and SQL-test query shapes
// against a real Snowflake, once with QUOTED_IDENTIFIERS_IGNORE_CASE = TRUE and
// once with it off, and asserts they read the same either way.
//
// The parameter is what a customer migrating off another warehouse leaves on:
// it stores every quoted identifier upper-cased, and SnowflakeDialect.Identifier
// quotes every alias our SQL asks for. Nothing on our side can turn it off —
// forcing it off per session would change the meaning of the SQL those customers
// write in their own tests — so the read has to work with it on.
//
// The unit reproducers in exec/stdsql pin the same behaviour against canned
// result sets. This suite is what proves the canned ones describe a real
// Snowflake rather than our idea of one.
type QuotedIdentifiersIgnoreCaseSuite struct {
	suite.Suite

	executor *SnowflakeExecutor

	database  string
	schema    string
	table     string
	numberCol string
	textCol   string
}

func TestQuotedIdentifiersIgnoreCaseSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping Snowflake integration tests in CI")
	}
	suite.Run(t, new(QuotedIdentifiersIgnoreCaseSuite))
}

func (s *QuotedIdentifiersIgnoreCaseSuite) SetupSuite() {
	s.database = os.Getenv("SNOWFLAKE_DATABASE")
	if s.database == "" {
		s.T().Skip("SNOWFLAKE_DATABASE env var not set")
	}
	s.schema = os.Getenv("SNOWFLAKE_SCHEMA")
	if s.schema == "" {
		s.schema = "PUBLIC"
	}

	conf := &SnowflakeConf{
		User:           os.Getenv("SNOWFLAKE_USER"),
		Password:       os.Getenv("SNOWFLAKE_PASSWORD"),
		Account:        os.Getenv("SNOWFLAKE_ACCOUNT"),
		Warehouse:      os.Getenv("SNOWFLAKE_WAREHOUSE"),
		Databases:      []string{s.database},
		Role:           os.Getenv("SNOWFLAKE_ROLE"),
		PrivateKeyFile: os.Getenv("SNOWFLAKE_PRIVATE_KEY_FILE"),
	}
	if pk := os.Getenv("SNOWFLAKE_PRIVATE_KEY"); pk != "" {
		conf.PrivateKey = []byte(pk)
	}

	executor, err := NewSnowflakeExecutor(context.Background(), conf)
	if err != nil {
		s.T().Skipf("Could not connect to Snowflake: %v", err)
	}
	s.executor = executor

	// The session parameter is set with ALTER SESSION, so every query has to run
	// on the session that ran it. One connection in the pool is what guarantees
	// that.
	s.executor.GetDb().SetMaxOpenConns(1)
	s.executor.GetDb().SetMaxIdleConns(1)

	s.discoverTable()
}

func (s *QuotedIdentifiersIgnoreCaseSuite) TearDownSuite() {
	if s.executor != nil {
		s.setIgnoreCase(false)
		_ = s.executor.Close()
	}
}

// discoverTable picks a small real table out of the configured schema and a
// numeric and a text column from it, so the queries below run over warehouse
// data rather than a literal we made up.
func (s *QuotedIdentifiersIgnoreCaseSuite) discoverTable() {
	ctx := context.Background()

	type tableRow struct {
		Name string `db:"TABLE_NAME"`
	}
	tables, err := NewQuerier[tableRow](s.executor).QueryMany(ctx, fmt.Sprintf(`
		SELECT TABLE_NAME
		FROM %s.INFORMATION_SCHEMA.TABLES
		WHERE TABLE_SCHEMA = '%s' AND TABLE_TYPE = 'BASE TABLE'
		  AND ROW_COUNT BETWEEN 1 AND 100000
		ORDER BY ROW_COUNT DESC, TABLE_NAME
		LIMIT 1`, s.database, s.schema))
	s.Require().NoError(err)
	if len(tables) == 0 {
		s.T().Skipf("no small BASE TABLE in %s.%s to read", s.database, s.schema)
	}
	s.table = tables[0].Name

	type columnRow struct {
		Name string `db:"COLUMN_NAME"`
		Type string `db:"DATA_TYPE"`
	}
	columns, err := NewQuerier[columnRow](s.executor).QueryMany(ctx, fmt.Sprintf(`
		SELECT COLUMN_NAME, DATA_TYPE
		FROM %s.INFORMATION_SCHEMA.COLUMNS
		WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s'
		ORDER BY ORDINAL_POSITION`, s.database, s.schema, s.table))
	s.Require().NoError(err)

	for _, column := range columns {
		switch {
		case s.numberCol == "" && column.Type == "NUMBER":
			s.numberCol = column.Name
		case s.textCol == "" && column.Type == "TEXT":
			s.textCol = column.Name
		}
	}
	if s.numberCol == "" || s.textCol == "" {
		s.T().Skipf("%s.%s.%s has no numeric and text column to profile", s.database, s.schema, s.table)
	}
	s.T().Logf("reading %s.%s.%s (numeric %s, text %s)", s.database, s.schema, s.table, s.numberCol, s.textCol)
}

// setIgnoreCase turns the account behaviour on and off for the session. Whether
// a real account has it on is not ours to choose, so the tests assert over both.
func (s *QuotedIdentifiersIgnoreCaseSuite) setIgnoreCase(on bool) {
	_, err := s.executor.GetDb().
		ExecContext(context.Background(), fmt.Sprintf("ALTER SESSION SET QUOTED_IDENTIFIERS_IGNORE_CASE = %t", on))
	s.Require().NoError(err)
}

// bothWays runs the body once per account behaviour, and first confirms the
// session really does what the subtest claims: an ALTER SESSION that silently
// did nothing would turn the folded half into a second copy of the ordinary one.
func (s *QuotedIdentifiersIgnoreCaseSuite) bothWays(body func(folded bool)) {
	for _, tc := range []struct {
		name   string
		folded bool
	}{
		{"account folds quoted identifiers", true},
		{"account keeps quoted identifiers as written", false},
	} {
		s.Run(tc.name, func() {
			s.setIgnoreCase(tc.folded)
			s.Require().Equal(tc.folded, s.aliasComesBackFolded())
			body(tc.folded)
		})
	}
}

func (s *QuotedIdentifiersIgnoreCaseSuite) aliasComesBackFolded() bool {
	rows, err := s.executor.QueryRows(context.Background(), `SELECT 1 AS "synq_probe"`)
	s.Require().NoError(err)
	defer rows.Close()
	columns, err := rows.Columns()
	s.Require().NoError(err)
	s.Require().Len(columns, 1)
	return columns[0] == "SYNQ_PROBE"
}

func (s *QuotedIdentifiersIgnoreCaseSuite) fqn() string {
	return fmt.Sprintf("%s.%s.%s", s.database, s.schema, s.table)
}

//
// Path A — the scan.
//

// The count query behind every SQL test. kernel-anomalies scans it into a struct
// with a `db:"num_failures"` tag, against an `AS "num_failures"` the account may
// have stored as NUM_FAILURES.
func (s *QuotedIdentifiersIgnoreCaseSuite) TestSqlTestCountQuery() {
	type countResult struct {
		Count int64 `ch:"num_failures" bigquery:"num_failures" db:"num_failures"`
	}

	s.bothWays(func(folded bool) {
		sql := fmt.Sprintf(`SELECT count(*) AS "num_failures" FROM %s WHERE %s IS NULL`, s.fqn(), s.numberCol)

		got, err := NewQuerier[countResult](s.executor).QueryMany(context.Background(), sql)

		s.Require().NoError(err)
		s.Require().Len(got, 1)
		s.GreaterOrEqual(got[0].Count, int64(0))
	})
}

// A volume monitor, with its query built by metrics/querybuilder rather than
// written out here, so what runs is what production sends.
func (s *QuotedIdentifiersIgnoreCaseSuite) TestVolumeMonitorQuery() {
	dialect := dwhsql.NewSnowflakeDialect()
	sql, err := querybuilder.
		NewQueryBuilder(dwhsql.TableFqn(s.database, s.schema, s.table), metrics.TableVolumeMetricsCols()).
		WithSegment(dwhsql.Identifier(s.textCol)).
		ToSql(dialect)
	s.Require().NoError(err)
	s.Contains(sql, `as "segment"`)
	s.Contains(sql, `as "num_rows"`)
	s.T().Log(sql)

	s.bothWays(func(folded bool) {
		got, err := NewQuerier[metrics.MetricVolume](s.executor).QueryMany(context.Background(), sql)

		s.Require().NoError(err)
		s.Require().NotEmpty(got, "the monitor produced no segments")

		var total int64
		for _, row := range got {
			s.GreaterOrEqual(row.NumRows, int64(0))
			total += row.NumRows
		}
		s.Positive(total, "every segment read zero rows, which is what the silent failure looks like")
	})
}

// The numeric field profile — the widest result shape a monitor scans, and the
// one that carries no segmentation, so `segment` and `time_segment` are absent
// from the result set and have to stay at their zero values.
func (s *QuotedIdentifiersIgnoreCaseSuite) TestNumericProfileQuery() {
	dialect := dwhsql.NewSnowflakeDialect()
	sql, err := querybuilder.NewQueryBuilder(
		dwhsql.TableFqn(s.database, s.schema, s.table),
		metrics.NumericMetricsCols(s.numberCol, dialect),
	).ToSql(dialect)
	s.Require().NoError(err)
	s.T().Log(sql)

	s.bothWays(func(folded bool) {
		got, err := NewQuerier[metrics.MetricNumericFieldStats](s.executor).QueryMany(context.Background(), sql)

		s.Require().NoError(err)
		s.Require().Len(got, 1)
		s.Equal(s.numberCol, got[0].Field)
		s.Positive(got[0].NumTotal)
		s.Require().NotNil(got[0].NumUnique)
		s.Positive(*got[0].NumUnique)
		s.Equal("", got[0].Segment)
		s.True(got[0].TimeSegment.IsZero())
	})
}

//
// Path B — the map lookup, which is the silent one.
//

// A SQL test with evaluators. The aggregates come back through QueryMaps and are
// read by the alias the query asked for; a miss reads as zero and the test
// reports green.
func (s *QuotedIdentifiersIgnoreCaseSuite) TestEvaluatorAggregatesThroughQueryMaps() {
	sql := fmt.Sprintf(`
		SELECT
			count_if(%[1]s IS NOT NULL) AS "fail_0",
			count_if(%[1]s IS NULL)     AS "fail_1",
			count_if(%[1]s IS NOT NULL) AS "fail_any"
		FROM %[2]s`, s.numberCol, s.fqn())

	s.bothWays(func(folded bool) {
		got, err := NewQuerier[exec.QueryMapResult](s.executor).QueryMaps(context.Background(), sql)

		s.Require().NoError(err)
		s.Require().Len(got, 1)
		row := got[0]

		failing, found := row.Get("fail_0")
		s.Require().True(found, "an evaluator's count must be readable by the alias the query asked for")
		s.Positive(toInt64(s.T(), failing), "the evaluator failed rows and must not report zero")

		total, found := row.Get("fail_any")
		s.Require().True(found)
		s.Positive(toInt64(s.T(), total))

		// A column that is genuinely not there stays a miss, so an alias we got
		// wrong is not papered over by the fold.
		_, found = row.Get("fail_2")
		s.False(found)

		// And the reason Get has to exist: on a folded account the key the
		// caller would have indexed with is not in the map at all, and the miss
		// raises nothing.
		_, foundRaw := row["fail_0"]
		s.Equal(!folded, foundRaw)
	})
}

func toInt64(t *testing.T, value any) int64 {
	t.Helper()
	switch v := value.(type) {
	case int64:
		return v
	case float64:
		return int64(v)
	case string:
		var n int64
		_, err := fmt.Sscan(v, &n)
		if err != nil {
			t.Fatalf("could not read %q as an integer: %v", v, err)
		}
		return n
	default:
		t.Fatalf("unexpected cell type %T", value)
		return 0
	}
}
