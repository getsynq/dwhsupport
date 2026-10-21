package scrapper

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	errTableGone    = errors.New("table is gone")
	errTableRefused = errors.New("permission denied for table")
)

func ddlTables(names ...string) []*SqlDefinitionRow {
	tables := make([]*SqlDefinitionRow, len(names))
	for i, name := range names {
		tables[i] = &SqlDefinitionRow{Instance: "host", Database: "db", Schema: "s", Table: name, IsTable: true}
	}
	return tables
}

// ddlOf answers each table by its name: a DDL, an error, or an empty DDL.
func ddlOf(answers map[string]any) func(context.Context, *SqlDefinitionRow) (string, error) {
	return func(_ context.Context, table *SqlDefinitionRow) (string, error) {
		switch a := answers[table.Table].(type) {
		case string:
			return a, nil
		case error:
			return "", a
		}
		return "", nil
	}
}

func isTableGone(err error) bool { return errors.Is(err, errTableGone) }

func tableNames(rows []*SqlDefinitionRow) []string {
	names := make([]string, len(rows))
	for i, row := range rows {
		names[i] = row.Table
	}
	return names
}

func TestReadTableDdlsKeepsTheOrderOfTheTables(t *testing.T) {
	answers := map[string]any{}
	var names []string
	for _, name := range []string{"f", "e", "d", "c", "b", "a"} {
		answers[name] = "CREATE TABLE " + name
		names = append(names, name)
	}
	// The first table answers last.
	read := func(ctx context.Context, table *SqlDefinitionRow) (string, error) {
		if table.Table == "f" {
			time.Sleep(20 * time.Millisecond)
		}
		return ddlOf(answers)(ctx, table)
	}

	rows, omissions, err := ReadTableDdls(context.Background(), ddlTables(names...), 3, read, isTableGone)
	require.NoError(t, err)
	assert.Empty(t, omissions)
	assert.Equal(t, names, tableNames(rows))
	for _, row := range rows {
		assert.Equal(t, "CREATE TABLE "+row.Table, row.Sql)
		assert.True(t, row.IsTable)
		assert.False(t, row.IsView)
	}
}

func TestReadTableDdlsWithoutTables(t *testing.T) {
	rows, omissions, err := ReadTableDdls(context.Background(), nil, 4, ddlOf(nil), isTableGone)
	require.NoError(t, err)
	assert.Empty(t, rows)
	assert.Empty(t, omissions)
}

// A table without a DDL is left out, and only a failure other than the table
// being dropped is reported, to a caller that accepts incomplete results.
func TestReadTableDdlsLeavesOutTablesWithoutDdl(t *testing.T) {
	answers := map[string]any{
		"kept":    "CREATE TABLE kept",
		"dropped": errTableGone,
		"refused": errTableRefused,
		"empty":   "",
	}
	tables := func() []*SqlDefinitionRow { return ddlTables("dropped", "empty", "kept", "refused") }

	t.Run("caller accepts incomplete results", func(t *testing.T) {
		rows, omissions, err := ReadTableDdls(WithIncompleteResults(context.Background()), tables(), 2, ddlOf(answers), isTableGone)
		require.NoError(t, err)
		assert.Equal(t, []string{"kept"}, tableNames(rows))
		assert.Equal(t, []Omission{{Database: "db", Schema: "s", Table: "refused", Part: OmittedDefinitions, Err: errTableRefused}}, omissions)
	})

	t.Run("caller does not", func(t *testing.T) {
		rows, omissions, err := ReadTableDdls(context.Background(), tables(), 2, ddlOf(answers), isTableGone)
		require.NoError(t, err)
		assert.Equal(t, []string{"kept"}, tableNames(rows))
		assert.Empty(t, omissions)
	})
}

func TestReadTableDdlsRunsAtMostParallelReads(t *testing.T) {
	var inFlight, most atomic.Int32
	read := func(context.Context, *SqlDefinitionRow) (string, error) {
		n := inFlight.Add(1)
		defer inFlight.Add(-1)
		for {
			m := most.Load()
			if n <= m || most.CompareAndSwap(m, n) {
				break
			}
		}
		time.Sleep(5 * time.Millisecond)
		return "CREATE TABLE t", nil
	}

	rows, _, err := ReadTableDdls(context.Background(), ddlTables("a", "b", "c", "d", "e", "f", "g", "h"), 3, read, isTableGone)
	require.NoError(t, err)
	assert.Len(t, rows, 8)
	assert.LessOrEqual(t, most.Load(), int32(3))
}

// A done context fails the read even when every table's error looks like a
// failure of that table alone, so a cancelled scan never passes for one that
// found no DDL.
func TestReadTableDdlsFailsWhenTheContextIsDone(t *testing.T) {
	ctx, cancel := context.WithCancel(WithIncompleteResults(context.Background()))
	read := func(ctx context.Context, table *SqlDefinitionRow) (string, error) {
		if table.Table == "b" {
			cancel()
		}
		<-ctx.Done()
		return "", errTableRefused
	}

	rows, omissions, err := ReadTableDdls(ctx, ddlTables("a", "b", "c"), 3, read, isTableGone)
	require.ErrorIs(t, err, context.Canceled)
	assert.Nil(t, rows)
	assert.Nil(t, omissions)
}
