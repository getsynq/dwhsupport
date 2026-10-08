package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/getsynq/dwhsupport/exec/querycontext"
	"github.com/getsynq/dwhsupport/querylogs"
	"github.com/getsynq/dwhsupport/testenv"
	"github.com/stretchr/testify/require"
)

// The executor sends our query context as the log_comment setting rather than
// as a SQL comment, so the query log has to carry log_comment for a reader to
// recognise our own queries.
func TestClickhouseQueryLogsCarryLogComment(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping ClickHouse query log tests in CI")
	}
	if testenv.EnvOrDefault("CLICKHOUSE_HOST", "") == "" {
		t.Skip("CLICKHOUSE_HOST env var not set")
	}
	sc, err := newClickhouseScrapperFromEnv(context.Background())
	if err != nil {
		t.Skipf("Could not connect to ClickHouse: %v", err)
	}
	defer sc.Close()

	from := time.Now().Add(-time.Minute)
	run := fmt.Sprintf("query-logs-%d", time.Now().UnixNano())
	marked := querycontext.WithQueryContext(context.Background(), querycontext.QueryContext{"app": "synq", "run": run})
	_, err = sc.QueryTables(marked)
	require.NoError(t, err)
	require.NoError(t, sc.executor.Exec(context.Background(), "SYSTEM FLUSH LOGS"))

	obfuscator, err := querylogs.NewQueryObfuscator(querylogs.ObfuscationNone)
	require.NoError(t, err)
	iter, err := sc.FetchQueryLogs(context.Background(), from, time.Now().Add(time.Minute), obfuscator)
	require.NoError(t, err)
	defer iter.Close()

	var found []string
	var weights []querylogs.QueryWeight
	for {
		log, err := iter.Next(context.Background())
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		logComment := log.Metadata.GetFields()["log_comment"].GetStringValue()
		if strings.Contains(logComment, run) {
			found = append(found, logComment)
			weights = append(weights, log.Weight())
		}
	}
	require.NotEmpty(t, found, "no query log of this run carries its log_comment")
	require.JSONEq(t, fmt.Sprintf(`{"app":"synq","run":%q}`, run), found[0])

	// The listing reads system tables, so it took time and read bytes.
	require.NotNil(t, weights[0].ExecutionMs)
	require.NotNil(t, weights[0].BytesScanned)
	require.Positive(t, *weights[0].BytesScanned)
	require.Nil(t, weights[0].ComputeMs)
}
