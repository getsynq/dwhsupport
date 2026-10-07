package athena

import (
	"context"
	"os"
	"testing"

	"github.com/getsynq/dwhsupport/exec/querycontext"
	"github.com/stretchr/testify/require"
)

// With a query context, the connection is checked with our own commented
// SELECT 1 rather than athenadriver's ping, and Athena runs it.
func TestAthenaConnectsWithQueryContext(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping Athena tests in CI")
	}
	if os.Getenv("ATHENA_ACCESS_KEY_ID") == "" || os.Getenv("ATHENA_SECRET_ACCESS_KEY") == "" {
		t.Skip("ATHENA_ACCESS_KEY_ID / ATHENA_SECRET_ACCESS_KEY env vars not set")
	}
	ctx := querycontext.WithQueryContext(context.Background(), querycontext.QueryContext{"source": "synq", "check": "connection"})
	sc, err := newAthenaScrapperFromEnv(ctx)
	require.NoError(t, err)
	defer sc.Close()

	databases, err := sc.QueryDatabases(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, databases)
}
