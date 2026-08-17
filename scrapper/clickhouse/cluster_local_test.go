package clickhouse

import (
	"context"
	"os"
	"testing"
	"time"

	dwhexecclickhouse "github.com/getsynq/dwhsupport/exec/clickhouse"
	"github.com/getsynq/dwhsupport/querylogs"
	"github.com/getsynq/dwhsupport/testenv"
	"github.com/stretchr/testify/suite"
)

// namedTestCluster is the extra cluster defined next to `default` by
// dev-infra/dwhtesting/lib/clickhouse, so that a cluster name other than the
// hardcoded one is exercised against a real server.
const namedTestCluster = "synq_test_cluster"

// readOnlyUser is the monitoring user created by the dwhtesting seed. It holds
// SELECT and SHOW on the test database and deliberately not REMOTE, which is what
// clusterAllReplicas needs — so it stands in for a warehouse whose owner would
// rather not hand that grant out.
const (
	readOnlyUser     = "synq"
	readOnlyPassword = "SynqTest1!"
)

// LocalClickHouseClusterSuite runs the scrapper against a local ClickHouse
// (dev-infra/dwhtesting/lib/clickhouse) in each cluster mode.
type LocalClickHouseClusterSuite struct {
	suite.Suite
	ctx          context.Context
	conf         dwhexecclickhouse.ClickhouseConf
	databaseName string
	clusters     []string
}

func TestLocalClickHouseClusterSuite(t *testing.T) {
	suite.Run(t, new(LocalClickHouseClusterSuite))
}

func (s *LocalClickHouseClusterSuite) SetupSuite() {
	if os.Getenv("CI") != "" {
		s.T().Skip("Skipping local ClickHouse tests in CI")
	}

	s.ctx = context.TODO()
	s.databaseName = testenv.EnvOrDefault("CLICKHOUSE_DATABASE", "synq_test")
	s.conf = dwhexecclickhouse.ClickhouseConf{
		Hostname:        testenv.EnvOrDefault("CLICKHOUSE_HOST", "127.0.0.1"),
		Port:            testenv.EnvOrDefaultInt("CLICKHOUSE_PORT", 9000),
		Username:        os.Getenv("CLICKHOUSE_USER"),
		Password:        testenv.EnvOrDefault("CLICKHOUSE_PASSWORD", "getsynq10"),
		DefaultDatabase: s.databaseName,
		NoSsl:           testenv.EnvOrDefaultBool("CLICKHOUSE_NO_SSL", true),
	}

	scrapper, err := NewClickhouseScrapper(s.ctx, ClickhouseScrapperConf{
		ClickhouseConf: s.conf,
		DatabaseName:   s.databaseName,
		Cluster:        ClusterConf{SingleNode: true},
	})
	if err != nil {
		s.T().Skipf("Skipping: could not connect to local ClickHouse: %v", err)
	}
	defer scrapper.Close()

	var exists uint8
	if err := scrapper.executor.GetDb().QueryRow(
		`SELECT 1 FROM system.tables WHERE database = ? AND name = 'test_clickhouse_scrapper'`,
		s.databaseName,
	).Scan(&exists); err != nil || exists != 1 {
		s.T().Skipf(
			"Skipping: dwhtesting fixtures not present in %q — run dev-infra/dwhtesting/reseed.sh clickhouse",
			s.databaseName,
		)
	}

	s.Require().NoError(
		scrapper.executor.Select(s.ctx, &s.clusters, `SELECT DISTINCT cluster FROM system.clusters ORDER BY cluster`),
	)
}

func (s *LocalClickHouseClusterSuite) scrapperFor(cluster ClusterConf) *ClickhouseScrapper {
	scrapper, err := NewClickhouseScrapper(s.ctx, ClickhouseScrapperConf{
		ClickhouseConf: s.conf,
		DatabaseName:   s.databaseName,
		Cluster:        cluster,
	})
	s.Require().NoError(err)
	s.T().Cleanup(func() { _ = scrapper.Close() })
	return scrapper
}

// assertScrapesMetadata runs every query that reads a ClickHouse system table, so
// that a query site left on the hardcoded cluster shows up here rather than on a
// customer's first catalog fetch.
func (s *LocalClickHouseClusterSuite) assertScrapesMetadata(scrapper *ClickhouseScrapper) {
	schemas, err := scrapper.QuerySchemas(s.ctx)
	s.Require().NoError(err)
	s.NotEmpty(schemas)

	tables, err := scrapper.QueryTables(s.ctx)
	s.Require().NoError(err)
	s.NotEmpty(tables)

	catalog, err := scrapper.QueryCatalog(s.ctx)
	s.Require().NoError(err)
	s.NotEmpty(catalog)

	metrics, err := scrapper.QueryTableMetrics(s.ctx, time.Time{})
	s.Require().NoError(err)
	s.NotEmpty(metrics)

	definitions, err := scrapper.QuerySqlDefinitions(s.ctx)
	s.Require().NoError(err)
	s.NotEmpty(definitions)

	constraints, err := scrapper.QueryTableConstraints(s.ctx)
	s.Require().NoError(err)
	s.NotEmpty(constraints)

	obfuscator, err := querylogs.NewQueryObfuscator(querylogs.ObfuscationNone)
	s.Require().NoError(err)
	logs, err := scrapper.FetchQueryLogs(s.ctx, time.Now().Add(-time.Hour), time.Now(), obfuscator)
	s.Require().NoError(err, "query logs read the same system tables")
	s.Require().NoError(logs.Close())
}

func (s *LocalClickHouseClusterSuite) TestUnsetClusterReadsThroughTheDefaultCluster() {
	if !s.hasCluster(DefaultClusterName) {
		s.T().Skipf("Skipping: this ClickHouse has no %q cluster", DefaultClusterName)
	}
	s.assertScrapesMetadata(s.scrapperFor(ClusterConf{}))
}

func (s *LocalClickHouseClusterSuite) TestNamedClusterReadsMetadata() {
	if !s.hasCluster(namedTestCluster) {
		s.T().Skipf(
			"Skipping: this ClickHouse has no %q cluster — apply dev-infra/dwhtesting/environments/staging",
			namedTestCluster,
		)
	}
	s.assertScrapesMetadata(s.scrapperFor(ClusterConf{Name: namedTestCluster}))
}

func (s *LocalClickHouseClusterSuite) TestSingleNodeReadsMetadata() {
	s.assertScrapesMetadata(s.scrapperFor(ClusterConf{SingleNode: true}))
}

func (s *LocalClickHouseClusterSuite) TestMissingClusterIsReportedBeforeItIsUsed() {
	scrapper := s.scrapperFor(ClusterConf{Name: "no_such_cluster"})

	warnings, err := scrapper.ValidateConfiguration(s.ctx)
	s.Require().NoError(err)
	s.Require().Len(warnings, 1)
	s.Contains(warnings[0], "no_such_cluster")

	_, err = scrapper.QueryTables(s.ctx)
	s.Error(err, "the cluster the warning names does not resolve")
}

func (s *LocalClickHouseClusterSuite) TestConfiguredClusterIsNotWarnedAbout() {
	if !s.hasCluster(DefaultClusterName) {
		s.T().Skipf("Skipping: this ClickHouse has no %q cluster", DefaultClusterName)
	}

	warnings, err := s.scrapperFor(ClusterConf{}).ValidateConfiguration(s.ctx)
	s.Require().NoError(err)
	s.Empty(warnings)

	warnings, err = s.scrapperFor(ClusterConf{SingleNode: true}).ValidateConfiguration(s.ctx)
	s.Require().NoError(err)
	s.Empty(warnings)
}

// TestSingleNodeNeedsNoRemotePrivilege is the reason single-node reads exist: the
// fan-out needs GRANT REMOTE ON *.*, and a read-only warehouse user usually does
// not have it.
func (s *LocalClickHouseClusterSuite) TestSingleNodeNeedsNoRemotePrivilege() {
	conf := s.conf
	conf.Username = readOnlyUser
	conf.Password = readOnlyPassword

	fanOut, err := NewClickhouseScrapper(s.ctx, ClickhouseScrapperConf{
		ClickhouseConf: conf,
		DatabaseName:   s.databaseName,
		Cluster:        ClusterConf{},
	})
	if err != nil {
		s.T().Skipf("Skipping: could not connect as %q — reseed dev-infra/dwhtesting: %v", readOnlyUser, err)
	}
	defer fanOut.Close()

	_, err = fanOut.QueryTables(s.ctx)
	s.Require().Error(err)
	s.True(fanOut.IsPermissionError(err), "expected a permission error, got %v", err)

	singleNode, err := NewClickhouseScrapper(s.ctx, ClickhouseScrapperConf{
		ClickhouseConf: conf,
		DatabaseName:   s.databaseName,
		Cluster:        ClusterConf{SingleNode: true},
	})
	s.Require().NoError(err)
	defer singleNode.Close()

	tables, err := singleNode.QueryTables(s.ctx)
	s.Require().NoError(err)
	s.NotEmpty(tables)
}

func (s *LocalClickHouseClusterSuite) hasCluster(name string) bool {
	for _, cluster := range s.clusters {
		if cluster == name {
			return true
		}
	}
	return false
}
