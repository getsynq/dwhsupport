package redshift

import (
	"context"
	"database/sql/driver"
	"testing"

	"github.com/getsynq/dwhsupport/exec"
	"github.com/getsynq/dwhsupport/exec/stdsql"
	"github.com/lib/pq"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

func queryN(t *testing.T, executor *RedshiftExecutor) (int, error) {
	t.Helper()
	rows, err := executor.QueryRows(context.Background(), "select n from t where x = $1", "a")
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	require.True(t, rows.Next())
	var n int
	require.NoError(t, rows.Scan(&n))
	return n, rows.Err()
}

func TestAQueryWorksAgainstTheFakeServer(t *testing.T) {
	s := newFakeServer(t, 0)
	n, err := queryN(t, connectToFake(t, s))
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.EqualValues(t, 1, s.executes.Load())
}

// The first query that needs compute on an idle Redshift Serverless workgroup
// is answered with BindComplete twice, which lib/pq takes for a broken
// connection. The query only reads, so it runs again on a fresh connection,
// which is what the next call did anyway.
func TestAQueryAnsweredWithADuplicateBindCompleteRunsAgain(t *testing.T) {
	s := newFakeServer(t, 1)
	executor := connectToFake(t, s)

	n, err := queryN(t, executor)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.EqualValues(t, 2, s.executes.Load(), "the query ran a second time")
	require.EqualValues(t, 2, s.connections.Load(), "the second run had a connection of its own")

	var selected []int
	require.NoError(t, executor.Select(context.Background(), &selected, "select n from t where x = $1", "a"))
	require.Equal(t, []int{1}, selected)
}

// An Exec may write, so it is never run twice: the error reaches the caller.
func TestAnExecAnsweredWithADuplicateBindCompleteIsNotRunAgain(t *testing.T) {
	s := newFakeServer(t, 1)
	executor := connectToFake(t, s)

	err := executor.Exec(context.Background(), "update t set n = 1 where x = $1", "a")
	require.ErrorContains(t, err, "unexpected message during extended query execution")
	require.EqualValues(t, 1, s.executes.Load())
}

// A server that keeps answering out of protocol is not retried forever:
// database/sql gives up after its own few attempts and the caller gets lib/pq's
// message, not a bare "bad connection".
func TestAQueryThatKeepsFailingReturnsTheDriverError(t *testing.T) {
	s := newFakeServer(t, 1000)
	executor := connectToFake(t, s)

	_, err := queryN(t, executor)
	require.ErrorContains(t, err, "pq: unexpected message during extended query execution: '2'")
	require.LessOrEqual(t, s.executes.Load(), int32(3))
}

func TestOnlyTheOutOfProtocolMessageMeansABrokenConnection(t *testing.T) {
	require.True(t, isUnexpectedExtendedQueryMessage(errors.New("pq: unexpected message during extended query execution: '2'")))
	require.False(t, isUnexpectedExtendedQueryMessage(nil))
	require.False(t, isUnexpectedExtendedQueryMessage(&pq.Error{Code: "42501", Message: "permission denied for relation svv_table_info"}))
	require.False(t, isUnexpectedExtendedQueryMessage(errors.New("pq: unexpected Bind response 'E'")))

	wrapped := &brokenReadError{err: errors.New("pq: unexpected message during extended query execution: '2'")}
	require.ErrorIs(t, wrapped, driver.ErrBadConn)
	require.Equal(t, "pq: unexpected message during extended query execution: '2'", wrapped.Error())
}

// Scrappers read through stdsql.QueryMany on GetDb(), QueryTableMetrics among
// them, so the retry has to sit below the executor's own methods.
func TestAReadThroughGetDbRunsAgain(t *testing.T) {
	s := newFakeServer(t, 1)
	executor := connectToFake(t, s)

	type row struct {
		N string `db:"n"`
	}
	rows, err := stdsql.QueryMany[row](context.Background(), executor.GetDb(), "select n from t where x = $1", exec.WithArgs[row]("a"))
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "1", rows[0].N)
	require.EqualValues(t, 2, s.executes.Load())
}
