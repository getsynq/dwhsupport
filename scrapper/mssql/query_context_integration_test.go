package mssql

import (
	"context"
	"os"
	"testing"

	"github.com/getsynq/dwhsupport/exec/querycontext"
	"github.com/stretchr/testify/require"
)

// With a query context, the connection is checked with our own commented
// SELECT 1 rather than go-mssqldb's ping, and the server accepts it.
func TestMSSQLConnectsWithQueryContext(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping MSSQL tests in CI")
	}
	ctx := querycontext.WithQueryContext(context.Background(), querycontext.QueryContext{"source": "synq", "check": "connection"})
	sc, err := newMSSQLScrapperFromEnv(ctx)
	if err != nil {
		t.Skipf("Could not connect to MSSQL: %v", err)
	}
	defer sc.Close()

	databases, err := sc.QueryDatabases(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, databases)
}
