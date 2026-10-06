package clickhouse

import (
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestPlatformUsersQueriesFollowTheClusterConf(t *testing.T) {
	queries := map[string]string{
		"users":               platformUsersSql,
		"users without valid": platformUsersNoValidUntilSql,
		"role grants":         platformUserRolesSql,
		"last login":          platformUserLastLoginSql,
	}
	for name, sql := range queries {
		t.Run(name, func(t *testing.T) {
			single := (&ClickhouseScrapper{conf: ClickhouseScrapperConf{Cluster: ClusterConf{SingleNode: true}}}).systemTablesSql(sql)
			assert.NotContains(t, single, "clusterAllReplicas", "single-node reads must not fan out")

			named := (&ClickhouseScrapper{conf: ClickhouseScrapperConf{Cluster: ClusterConf{Name: "prod"}}}).systemTablesSql(sql)
			assert.Contains(t, named, "('prod', system.", "a named cluster replaces the default one")
		})
	}
}

func TestPlatformUsersSkipReason(t *testing.T) {
	e := &ClickhouseScrapper{}

	refused := errors.Wrap(&clickhouse.Exception{Code: 497, Message: "u: Not enough privileges."}, "query")
	assert.Equal(t, "refused, needs SELECT ON system.role_grants", skipReason(e, refused, "SELECT ON system.role_grants"))

	other := errors.New("connection reset\nstack")
	assert.Equal(t, "failed: connection reset", skipReason(e, other, "SELECT ON system.role_grants"))
}

func TestIsClickhouseErrCode(t *testing.T) {
	missing := errors.Wrap(&clickhouse.Exception{Code: chErrUnknownTable}, "query")
	assert.True(t, isClickhouseErrCode(missing, chErrUnknownTable))
	assert.False(t, isClickhouseErrCode(missing, chErrUnknownIdentifier))
	assert.False(t, isClickhouseErrCode(errors.New("Table system.session_log does not exist"), chErrUnknownTable))
}
