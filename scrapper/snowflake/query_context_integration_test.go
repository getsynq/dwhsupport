package snowflake

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/getsynq/dwhsupport/exec/querycontext"
	"github.com/stretchr/testify/require"
)

// The statement that checks a new connection shows in the account's query
// history with our query tag and comment, like every other statement we send.
func TestSnowflakeConnectionCheckCarriesTheQueryContext(t *testing.T) {
	skipWithoutSnowflake(t)
	qc := querycontext.QueryContext{"source": "synq", "run": fmt.Sprintf("connection-check-%d", time.Now().UnixNano())}
	sc, err := NewSnowflakeScrapper(
		querycontext.WithQueryContext(context.Background(), qc),
		&SnowflakeScrapperConf{SnowflakeConf: snowflakeTestConf()},
	)
	if err != nil {
		t.Skipf("Could not connect to Snowflake: %v", err)
	}
	defer sc.Close()

	// The pool keeps the connection the check opened, so the check is in this
	// session's history.
	var rows []struct {
		QueryText string `db:"QUERY_TEXT"`
		QueryTag  string `db:"QUERY_TAG"`
	}
	require.NoError(t, sc.Executor().Select(context.Background(), &rows,
		`SELECT query_text, query_tag FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY_BY_SESSION()) WHERE query_tag = ?`,
		qc.FormatAsJSON()))
	require.Len(t, rows, 1, "exactly one statement ran with the tag: the connection check")
	require.Equal(t, querycontext.AppendSQLComment(querycontext.WithQueryContext(context.Background(), qc), "SELECT 1"), rows[0].QueryText)
}
