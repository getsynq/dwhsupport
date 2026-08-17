package clickhouse

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/getsynq/dwhsupport/querylogs"
	"github.com/stretchr/testify/suite"
)

type ClusterConfSuite struct {
	suite.Suite
}

func TestClusterConfSuite(t *testing.T) {
	suite.Run(t, new(ClusterConfSuite))
}

func (s *ClusterConfSuite) TestValidate() {
	s.NoError(ClusterConf{}.Validate())
	s.NoError(ClusterConf{Name: "analytics_cluster"}.Validate())
	s.NoError(ClusterConf{Name: "prod-eu.1"}.Validate())
	s.NoError(ClusterConf{SingleNode: true}.Validate())

	s.Error(ClusterConf{Name: "1cluster"}.Validate())
	s.Error(ClusterConf{Name: "my cluster"}.Validate())
	s.Error(ClusterConf{Name: "a', system.tables) UNION ALL SELECT 1 FROM ('"}.Validate())
	s.Error(ClusterConf{SingleNode: true, Name: "analytics"}.Validate(),
		"a cluster name alongside single-node reads means one of the two was not meant")
}

func (s *ClusterConfSuite) TestResolveSystemTablesDefaultsToTheDefaultCluster() {
	s.Equal(
		"SELECT name FROM clusterAllReplicas('default', system.tables)",
		ClusterConf{}.resolveSystemTables("SELECT name FROM clusterAllReplicas(default, system.tables)"),
	)
}

func (s *ClusterConfSuite) TestResolveSystemTablesNamedCluster() {
	s.Equal(
		"SELECT name FROM clusterAllReplicas('analytics', system.tables)",
		ClusterConf{Name: "analytics"}.resolveSystemTables("SELECT name FROM clusterAllReplicas(default, system.tables)"),
	)
}

func (s *ClusterConfSuite) TestResolveSystemTablesSingleNode() {
	s.Equal(
		"SELECT name FROM system.tables",
		ClusterConf{SingleNode: true}.resolveSystemTables("SELECT name FROM clusterAllReplicas(default, system.tables)"),
	)
}

func (s *ClusterConfSuite) TestResolveSystemTablesRewritesEveryOccurrence() {
	sql := `SELECT * FROM clusterAllReplicas(default, system.tables) t
	        JOIN clusterAllReplicas( default , system.data_skipping_indices ) i USING (database)
	        JOIN clusterAllReplicas(default, system.parts) p USING (database)`

	resolved := ClusterConf{SingleNode: true}.resolveSystemTables(sql)

	s.NotContains(resolved, "clusterAllReplicas")
	s.Contains(resolved, "system.tables t")
	s.Contains(resolved, "system.data_skipping_indices i")
	s.Contains(resolved, "system.parts p")
}

func (s *ClusterConfSuite) TestResolveSystemTablesLeavesOtherTablesAlone() {
	sql := "SELECT * FROM synq_test.products JOIN clusterAllReplicas(default, system.tables) USING (name)"

	s.Equal(
		"SELECT * FROM synq_test.products JOIN system.tables USING (name)",
		ClusterConf{SingleNode: true}.resolveSystemTables(sql),
	)
}

// systemTableQueries is every query this scrapper sends against a ClickHouse
// system table. A query that is not here is not rewritten for the configured
// cluster, so it reads the hardcoded default one — TestEverySqlFileIsRegistered
// fails when a new .sql file is added without being listed.
func systemTableQueries() map[string]string {
	scrapper := &ClickhouseScrapper{}
	return map[string]string{
		"query_catalog.sql":           queryCatalogSql,
		"query_schemas.sql":           querySchemasSql,
		"query_tables.sql":            queryTablesSql,
		"query_table_constraints.sql": queryTableConstraintsSql,
		"query_sql_definitions.sql":   querySqlDefinitionsSql,
		"query_table_metrics.sql":     queryTableMetricsSql,
		"query_logs.go":               scrapper.buildQueryLogsSql(querylogs.ObfuscationNone),
	}
}

func (s *ClusterConfSuite) TestEverySqlFileIsRegistered() {
	files, err := filepath.Glob("*.sql")
	s.Require().NoError(err)
	s.Require().NotEmpty(files)

	registered := systemTableQueries()
	for _, file := range files {
		if _, ok := registered[file]; ok {
			continue
		}
		body, err := os.ReadFile(file)
		s.Require().NoError(err)
		s.NotContains(string(body), "system.", "%s reads system tables but is not registered in systemTableQueries", file)
	}
}

func (s *ClusterConfSuite) TestEveryQueryIsRewritableForSingleNode() {
	for name, sql := range systemTableQueries() {
		s.Run(name, func() {
			resolved := ClusterConf{SingleNode: true}.resolveSystemTables(sql)
			s.NotContains(resolved, "clusterAllReplicas",
				"a reference the resolver does not match stays pinned to the default cluster")
		})
	}
}

func (s *ClusterConfSuite) TestEveryQueryIsRewritableForANamedCluster() {
	for name, sql := range systemTableQueries() {
		s.Run(name, func() {
			resolved := ClusterConf{Name: "analytics"}.resolveSystemTables(sql)
			s.NotContains(resolved, "clusterAllReplicas(default",
				"a reference the resolver does not match stays pinned to the default cluster")
		})
	}
}

// TestSourcesUseTheCanonicalSystemTableForm guards the shape the resolver keys
// off: a query written any other way parses and runs, and silently ignores the
// cluster configuration.
func (s *ClusterConfSuite) TestSourcesUseTheCanonicalSystemTableForm() {
	sources, err := filepath.Glob("*.sql")
	s.Require().NoError(err)
	goFiles, err := filepath.Glob("query_*.go")
	s.Require().NoError(err)
	sources = append(sources, goFiles...)

	for _, source := range sources {
		body, err := os.ReadFile(source)
		s.Require().NoError(err)

		text := string(body)
		occurrences := strings.Count(text, "clusterAllReplicas(")
		matched := len(systemTableRefPattern.FindAllString(text, -1))
		s.Equal(occurrences, matched,
			"%s has a clusterAllReplicas() reference not in the form the resolver rewrites", source)
	}
}
