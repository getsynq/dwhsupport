package clickhouse

import (
	"context"
	"fmt"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func chErr(code int32, message string) error {
	return errors.Wrap(&clickhouse.Exception{Code: code, Message: message}, "query")
}

var (
	errRefusedUsers   = chErr(497, "u: Not enough privileges. To execute this query, it's necessary to have the grant SELECT ON system.users")
	errRefusedRemote  = chErr(497, "u: Not enough privileges. To execute this query, it's necessary to have the grant READ ON REMOTE")
	errNoCluster      = chErr(chErrClusterDoesntExist, "Requested cluster 'default' not found")
	errNoClusterOld   = chErr(chErrBadGet, "Requested cluster 'default' not found")
	errNoTable        = chErr(chErrUnknownTable, "Table system.session_log does not exist")
	errNoColumn       = chErr(chErrUnknownIdentifier, "Unknown expression identifier `valid_until`")
	errNoSuchColumn   = chErr(chErrNoSuchColumnInTable, "There is no column valid_until in table")
	errScalarNotArray = chErr(chErrIllegalTypeOfArgument, "Argument 2 of function arrayAll must be array")
	errTimeout        = chErr(159, "Timeout exceeded")
	errDropped        = errors.New("read: connection reset by peer")
)

func TestPlatformUsersQueriesFollowTheClusterConf(t *testing.T) {
	queries := map[string]string{
		"role grants": platformUserRolesSql,
		"last login":  platformUserLastLoginSql,
	}
	for i, q := range platformUsersQueries {
		queries[fmt.Sprintf("users variant %d", i)] = q.sql
	}
	for name, sql := range queries {
		t.Run(name, func(t *testing.T) {
			single := (&ClickhouseScrapper{conf: ClickhouseScrapperConf{Cluster: ClusterConf{SingleNode: true}}}).systemTablesSql(sql)
			assert.NotContains(t, single, "clusterAllReplicas", "single-node reads must not fan out")

			named := (&ClickhouseScrapper{conf: ClickhouseScrapperConf{Cluster: ClusterConf{Name: "prod"}}}).systemTablesSql(sql)
			assert.Contains(t, named, "('prod', system.", "a named cluster replaces the default one")
		})
	}
	assert.True(t, platformUsersQueries[len(platformUsersQueries)-1].noDisabled, "the last variant reads what every version has")
}

// TestPlatformUsersSourceClassification pins which errors of the system.users
// read make a refused, an unavailable or a failed source.
func TestPlatformUsersSourceClassification(t *testing.T) {
	e := &ClickhouseScrapper{}
	classify := func(err error) *scrapper.PlatformUserListing {
		return scrapper.PlatformUserSourceError(platformUsersSource, scrapper.PlatformUserSourceSQL, err, e.IsPermissionError,
			isUnavailableOnThisServer)
	}

	for name, err := range map[string]error{"system.users": errRefusedUsers, "remote": errRefusedRemote} {
		t.Run("refused "+name, func(t *testing.T) {
			src := classify(err)
			assert.NotEmpty(t, src.Refused)
			assert.Empty(t, src.Unavailable)
			assert.Empty(t, src.Failed)
		})
	}
	for name, err := range map[string]error{"table": errNoTable, "identifier": errNoColumn, "column": errNoSuchColumn, "type": errScalarNotArray} {
		t.Run("unavailable "+name, func(t *testing.T) {
			src := classify(err)
			assert.Empty(t, src.Refused)
			assert.NotEmpty(t, src.Unavailable)
			assert.Empty(t, src.Failed)
		})
	}
	for name, err := range map[string]error{"timeout": errTimeout, "connection": errDropped} {
		t.Run("failed "+name, func(t *testing.T) {
			src := classify(err)
			assert.Empty(t, src.Refused)
			assert.Empty(t, src.Unavailable)
			assert.NotEmpty(t, src.Failed)
		})
	}
}

// A refused or unavailable system.users is a result, a failed one with nothing
// else answering is the call's error.
func TestPlatformUsersCollect(t *testing.T) {
	e := &ClickhouseScrapper{}
	collect := func(err error) (*scrapper.PlatformUsers, error) {
		return scrapper.CollectPlatformUsers(context.Background(),
			scrapper.PlatformUserSourceError(platformUsersSource, scrapper.PlatformUserSourceSQL, err, e.IsPermissionError,
				isUnavailableOnThisServer))
	}

	users, err := collect(errRefusedUsers)
	require.NoError(t, err)
	assert.NotEmpty(t, users.Reconcile().Refused)

	users, err = collect(errNoTable)
	require.NoError(t, err)
	assert.NotEmpty(t, users.Reconcile().Unavailable)

	users, err = collect(errDropped)
	require.Error(t, err)
	assert.Nil(t, users)
}

func TestPlatformUsersFanOutFallback(t *testing.T) {
	cluster := &ClickhouseScrapper{}
	single := &ClickhouseScrapper{conf: ClickhouseScrapperConf{Cluster: ClusterConf{SingleNode: true}}}

	scope, ok := cluster.fanOutFallback(errRefusedRemote)
	assert.True(t, ok)
	assert.Equal(t, readLocalRemoteRefused, scope)

	for _, err := range []error{errNoCluster, errNoClusterOld} {
		scope, ok = cluster.fanOutFallback(err)
		assert.True(t, ok)
		assert.Equal(t, readLocalNoCluster, scope)
	}

	for _, err := range []error{errRefusedUsers, errNoTable, errTimeout, errDropped} {
		_, ok = cluster.fanOutFallback(err)
		assert.Falsef(t, ok, "%v is not about the fan-out, a local read would fail the same way", err)
	}

	_, ok = single.fanOutFallback(errRefusedRemote)
	assert.False(t, ok, "a single-node read has no fan-out to fall back from")
}

func TestPlatformUsersFactSkipReason(t *testing.T) {
	e := &ClickhouseScrapper{}
	assert.Equal(t, "refused, needs SELECT ON system.role_grants", e.factSkipReason(errRefusedUsers, "SELECT ON system.role_grants"))
	assert.Contains(t, e.factSkipReason(errNoColumn, "x"), "unavailable on this ClickHouse version")
	assert.Equal(t, "failed: connection reset", e.factSkipReason(errors.New("connection reset\nstack"), "x"))
}

func TestIsClickhouseErrCode(t *testing.T) {
	assert.True(t, isClickhouseErrCode(errNoTable, chErrUnknownTable))
	assert.True(t, isClickhouseErrCode(errNoTable, chErrUnknownIdentifier, chErrUnknownTable))
	assert.False(t, isClickhouseErrCode(errNoTable, chErrUnknownIdentifier))
	assert.False(t, isClickhouseErrCode(errors.New("Table system.session_log does not exist"), chErrUnknownTable))
}
