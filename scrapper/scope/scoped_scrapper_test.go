package scope

import (
	"context"
	"testing"
	"time"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/sqldialect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockScrapper is a minimal mock for testing ScopedScrapper.
type mockScrapper struct {
	catalogRows    []*scrapper.CatalogColumnRow
	tableRows      []*scrapper.TableRow
	metricsRows    []*scrapper.TableMetricsRow
	sqlDefRows     []*scrapper.SqlDefinitionRow
	databaseRows   []*scrapper.DatabaseRow
	constraintRows []*scrapper.TableConstraintRow
}

func (m *mockScrapper) DialectType() string            { return "mock" }
func (m *mockScrapper) SqlDialect() sqldialect.Dialect { return nil }
func (m *mockScrapper) IsPermissionError(error) bool   { return false }
func (m *mockScrapper) Close() error                   { return nil }

func (m *mockScrapper) ValidateConfiguration(context.Context) ([]string, error) {
	return nil, nil
}

func (m *mockScrapper) QueryCatalog(context.Context) ([]*scrapper.CatalogColumnRow, error) {
	return m.catalogRows, nil
}

func (m *mockScrapper) QueryTableMetrics(_ context.Context, _ time.Time) ([]*scrapper.TableMetricsRow, error) {
	return m.metricsRows, nil
}

func (m *mockScrapper) QuerySqlDefinitions(context.Context) ([]*scrapper.SqlDefinitionRow, error) {
	return m.sqlDefRows, nil
}

func (m *mockScrapper) QueryTables(context.Context) ([]*scrapper.TableRow, error) {
	return m.tableRows, nil
}

func (m *mockScrapper) QueryDatabases(context.Context) ([]*scrapper.DatabaseRow, error) {
	return m.databaseRows, nil
}

func (m *mockScrapper) QuerySegments(context.Context, string, ...any) ([]*scrapper.SegmentRow, error) {
	return nil, nil
}

func (m *mockScrapper) QueryCustomMetrics(context.Context, string, ...any) ([]*scrapper.CustomMetricsRow, error) {
	return nil, nil
}

func (m *mockScrapper) QueryShape(context.Context, string) ([]*scrapper.QueryShapeColumn, error) {
	return nil, nil
}

func (m *mockScrapper) QueryTableConstraints(context.Context) ([]*scrapper.TableConstraintRow, error) {
	return m.constraintRows, nil
}

func TestScopedScrapper_QueryCatalog(t *testing.T) {
	inner := &mockScrapper{
		catalogRows: []*scrapper.CatalogColumnRow{
			{Database: "prod", Schema: "public", Table: "users", Column: "id"},
			{Database: "prod", Schema: "internal", Table: "secrets", Column: "key"},
			{Database: "dev", Schema: "public", Table: "users", Column: "id"},
		},
	}

	ss := NewScopedScrapper(inner, &ScopeFilter{
		Include: []ScopeRule{{Database: "prod"}},
		Exclude: []ScopeRule{{Schema: "internal"}},
	})

	rows, err := ss.QueryCatalog(context.Background())
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, "public", rows[0].Schema)
}

func TestScopedScrapper_QueryTables(t *testing.T) {
	inner := &mockScrapper{
		tableRows: []*scrapper.TableRow{
			{Database: "prod", Schema: "public", Table: "users"},
			{Database: "dev", Schema: "public", Table: "users"},
		},
	}

	ss := NewScopedScrapper(inner, &ScopeFilter{
		Include: []ScopeRule{{Database: "prod"}},
	})

	rows, err := ss.QueryTables(context.Background())
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, "prod", rows[0].Database)
}

func TestScopedScrapper_QueryDatabases(t *testing.T) {
	inner := &mockScrapper{
		databaseRows: []*scrapper.DatabaseRow{
			{Database: "prod"},
			{Database: "dev"},
			{Database: "analytics"},
		},
	}

	ss := NewScopedScrapper(inner, &ScopeFilter{
		Include: []ScopeRule{{Database: "prod"}, {Database: "analytics"}},
	})

	rows, err := ss.QueryDatabases(context.Background())
	require.NoError(t, err)
	require.Len(t, rows, 2)
	assert.Equal(t, "prod", rows[0].Database)
	assert.Equal(t, "analytics", rows[1].Database)
}

func TestScopedScrapper_NilBaseScope(t *testing.T) {
	inner := &mockScrapper{
		tableRows: []*scrapper.TableRow{
			{Database: "any", Schema: "any", Table: "any"},
		},
	}

	ss := NewScopedScrapper(inner, nil)

	rows, err := ss.QueryTables(context.Background())
	require.NoError(t, err)
	assert.Len(t, rows, 1)
}

func TestScopedScrapper_ContextNarrowing(t *testing.T) {
	inner := &mockScrapper{
		catalogRows: []*scrapper.CatalogColumnRow{
			{Database: "prod", Schema: "public", Table: "users", Column: "id"},
			{Database: "prod", Schema: "raw", Table: "events", Column: "ts"},
			{Database: "analytics", Schema: "public", Table: "metrics", Column: "val"},
		},
	}

	ss := NewScopedScrapper(inner, &ScopeFilter{
		Include: []ScopeRule{{Database: "prod"}, {Database: "analytics"}},
	})

	// Narrow via context: only public schema.
	ctx := WithScope(context.Background(), &ScopeFilter{
		Include: []ScopeRule{{Schema: "public"}},
	})

	rows, err := ss.QueryCatalog(ctx)
	require.NoError(t, err)
	require.Len(t, rows, 2)
	assert.Equal(t, "prod", rows[0].Database)
	assert.Equal(t, "analytics", rows[1].Database)
}

func TestScopedScrapper_CallerCannotWiden(t *testing.T) {
	inner := &mockScrapper{
		tableRows: []*scrapper.TableRow{
			{Database: "prod", Schema: "public", Table: "users"},
			{Database: "dev", Schema: "public", Table: "users"},
		},
	}

	// Base scope: only prod.
	ss := NewScopedScrapper(inner, &ScopeFilter{
		Include: []ScopeRule{{Database: "prod"}},
	})

	// Caller tries to widen to include dev — should still be blocked by base scope.
	ctx := WithScope(context.Background(), &ScopeFilter{
		Include: []ScopeRule{{Database: "prod"}, {Database: "dev"}},
	})

	rows, err := ss.QueryTables(ctx)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, "prod", rows[0].Database)
}
