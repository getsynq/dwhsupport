package redshift

import (
	"context"
	"testing"

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
	s := newFakeServer(t, false)
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
	s := newFakeServer(t, true)
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
	s := newFakeServer(t, true)
	executor := connectToFake(t, s)

	err := executor.Exec(context.Background(), "update t set n = 1 where x = $1", "a")
	require.ErrorContains(t, err, "unexpected message during extended query execution")
	require.EqualValues(t, 1, s.executes.Load())
}
