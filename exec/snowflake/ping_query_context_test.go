package snowflake

import (
	"context"
	"strings"
	"testing"

	"github.com/getsynq/dwhsupport/exec/querycontext"
	gosnowflake "github.com/snowflakedb/gosnowflake"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The statement that checks a new connection reaches the account's query
// history like any other, so it carries the caller's query context: the query
// tag and the SQL comment.
func TestConnectionCheckCarriesTheQueryContext(t *testing.T) {
	fake := &fakeSnowflake{loginBody: func(string) string { return fakeLoginOK }}
	qc := querycontext.QueryContext{"source": "synq", "monitor": "m-1"}
	ctx := querycontext.WithQueryContext(context.Background(), qc)

	execer, err := NewSnowflakeExecutor(ctx, fakeConf(fake, "GOOD_DB"))
	require.NoError(t, err)
	defer func() { _ = execer.Close() }()

	queries := fake.queryRequests()
	require.Len(t, queries, 1, "one statement checks the connection")
	assert.Equal(t, qc.FormatAsJSON(), queries[0].Parameters["QUERY_TAG"])
	assert.Equal(t, querycontext.AppendSQLComment(ctx, "SELECT 1"), queries[0].SQLText)
}

// The reconnect without the default database checks its connection the same way.
func TestConnectionCheckAfterReconnectCarriesTheQueryContext(t *testing.T) {
	fake := &fakeSnowflake{loginBody: func(query string) string {
		if strings.Contains(query, "databaseName=NO_SUCH_DB") {
			return fakeLoginFailure(gosnowflake.ErrObjectNotExistOrAuthorized, "The requested database does not exist or not authorized.")
		}
		return fakeLoginOK
	}}
	qc := querycontext.QueryContext{"source": "synq"}
	ctx := querycontext.WithQueryContext(context.Background(), qc)

	execer, err := NewSnowflakeExecutor(ctx, fakeConf(fake, "NO_SUCH_DB"))
	require.NoError(t, err)
	defer func() { _ = execer.Close() }()

	require.Len(t, fake.attempts(), 2)
	queries := fake.queryRequests()
	require.Len(t, queries, 1)
	assert.Equal(t, qc.FormatAsJSON(), queries[0].Parameters["QUERY_TAG"])
	assert.Equal(t, querycontext.AppendSQLComment(ctx, "SELECT 1"), queries[0].SQLText)
}

// Without a query context the check goes out as it always did.
func TestConnectionCheckWithoutQueryContextIsUntagged(t *testing.T) {
	fake := &fakeSnowflake{loginBody: func(string) string { return fakeLoginOK }}

	execer, err := NewSnowflakeExecutor(context.Background(), fakeConf(fake, "GOOD_DB"))
	require.NoError(t, err)
	defer func() { _ = execer.Close() }()

	queries := fake.queryRequests()
	require.Len(t, queries, 1)
	assert.NotContains(t, queries[0].Parameters, "QUERY_TAG")
	assert.Equal(t, "SELECT 1", queries[0].SQLText)
}
