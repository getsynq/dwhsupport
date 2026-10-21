package redshift

import (
	"context"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	dwhexecredshift "github.com/getsynq/dwhsupport/exec/redshift"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/testenv"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

// RedshiftSystemObjectsSuite checks that no listing carries what Redshift keeps
// for itself: its system schemas, and the table that stores a materialized
// view's rows. dwhtesting's pipelines.events_hourly is a materialized view, so
// its backing table pipelines.mv_tbl__events_hourly__0 (owned by rdsdb) is
// there to be left out, and pg_auto_copy.copy_job_detail is visible to every
// user. The fixture tables of schema_a must still be listed.
type RedshiftSystemObjectsSuite struct {
	suite.Suite
	scrapper *RedshiftScrapper
}

func TestRedshiftSystemObjectsSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping Redshift system object tests in CI")
	}
	suite.Run(t, new(RedshiftSystemObjectsSuite))
}

func (s *RedshiftSystemObjectsSuite) SetupSuite() {
	host := testenv.EnvOrDefault("REDSHIFT_HOST", "")
	if host == "" {
		s.T().Skip("REDSHIFT_HOST env var not set")
	}
	port, err := strconv.Atoi(testenv.EnvOrDefault("REDSHIFT_PORT", "5439"))
	s.Require().NoError(err)
	sc, err := NewRedshiftScrapper(context.Background(), &RedshiftScrapperConf{
		RedshiftConf: dwhexecredshift.RedshiftConf{
			Host:     host,
			Port:     port,
			User:     testenv.EnvOrDefault("REDSHIFT_USER", ""),
			Password: testenv.EnvOrDefault("REDSHIFT_PASSWORD", ""),
			Database: testenv.EnvOrDefault("REDSHIFT_DATABASE", "dev"),
		},
	})
	if err != nil {
		s.T().Skipf("Could not connect to Redshift: %v", err)
	}
	s.scrapper = sc
}

func (s *RedshiftSystemObjectsSuite) TearDownSuite() {
	if s.scrapper != nil {
		_ = s.scrapper.Close()
	}
}

func TestEveryCatalogQueryLeavesOutTheSystemSchemas(t *testing.T) {
	for name, sql := range map[string]string{
		"tables":             withSystemSchemas(tablesSql),
		"schemas":            withSystemSchemas(schemasSql),
		"catalog":            withSystemSchemas(queryCatalogSql),
		"sql definitions":    withSystemSchemas(querySqlDefinitionsSql),
		"constraints":        withSystemSchemas(queryTableConstraintsSql),
		"sort and dist keys": withSystemSchemas(querySortDistKeysSql),
		"metrics":            withSystemSchemas(queryTableMetricsSql),
		"metrics, freshness": withSystemSchemas(queryTabkeMetricsEstimatedFreshnessSql),
	} {
		require.NotContains(t, sql, systemSchemasPlaceholder, name)
		for _, schema := range systemSchemaNames {
			require.Contains(t, sql, "'"+schema+"'", name)
		}
	}
	for name, sql := range map[string]string{
		"tables":             tablesSql,
		"catalog":            queryCatalogSql,
		"constraints":        queryTableConstraintsSql,
		"sort and dist keys": querySortDistKeysSql,
	} {
		require.Contains(t, sql, "'mv_tbl__'", name)
	}
}

// systemSchemaNames are the schemas Redshift creates for itself.
var systemSchemaNames = []string{
	"pg_catalog", "information_schema", "catalog_history", "pg_internal", "pg_toast",
	"pg_automv", "pg_auto_copy", "pg_mv", "pg_s3",
}

func (s *RedshiftSystemObjectsSuite) checkRows(rows []scrapper.DwhFqn, expectFixture bool) {
	fixtureSeen := false
	for _, fqn := range rows {
		s.NotContains(systemSchemaNames, fqn.SchemaName, "system schema object %s is listed", fqn)
		s.False(strings.HasPrefix(fqn.ObjectName, "mv_tbl__"), "materialized view backing table %s is listed", fqn)
		if fqn.SchemaName == "schema_a" {
			fixtureSeen = true
		}
	}
	if expectFixture {
		s.True(fixtureSeen, "no object of the fixture schema schema_a is listed")
	}
}

func fqnsOf[T scrapper.HasTableFqn](rows []T) []scrapper.DwhFqn {
	out := make([]scrapper.DwhFqn, len(rows))
	for i, row := range rows {
		out[i] = row.TableFqn()
	}
	return out
}

func (s *RedshiftSystemObjectsSuite) TestQueryTables() {
	rows, err := s.scrapper.QueryTables(context.Background())
	s.Require().NoError(err)
	s.checkRows(fqnsOf(rows), true)
}

func (s *RedshiftSystemObjectsSuite) TestQueryCatalog() {
	rows, err := s.scrapper.QueryCatalog(context.Background())
	s.Require().NoError(err)
	s.checkRows(fqnsOf(rows), true)
}

func (s *RedshiftSystemObjectsSuite) TestQueryTableMetrics() {
	rows, err := s.scrapper.QueryTableMetrics(context.Background(), time.Time{})
	s.Require().NoError(err)
	s.checkRows(fqnsOf(rows), true)
}

func (s *RedshiftSystemObjectsSuite) TestQuerySqlDefinitions() {
	rows, err := s.scrapper.QuerySqlDefinitions(context.Background())
	s.Require().NoError(err)
	s.checkRows(fqnsOf(rows), false)
}

func (s *RedshiftSystemObjectsSuite) TestQueryTableConstraints() {
	rows, err := s.scrapper.QueryTableConstraints(context.Background())
	s.Require().NoError(err)
	s.checkRows(fqnsOf(rows), true)
}

func (s *RedshiftSystemObjectsSuite) TestQuerySchemas() {
	rows, err := s.scrapper.QuerySchemas(context.Background())
	s.Require().NoError(err)
	fqns := make([]scrapper.DwhFqn, len(rows))
	for i, row := range rows {
		fqns[i] = scrapper.DwhFqn{DatabaseName: row.Database, SchemaName: row.Schema}
	}
	s.checkRows(fqns, true)
}
