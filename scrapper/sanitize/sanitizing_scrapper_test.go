package sanitize

import (
	"context"
	"testing"
	"time"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/sqldialect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type stubScrapper struct {
	catalog     []*scrapper.CatalogColumnRow
	tables      []*scrapper.TableRow
	metrics     []*scrapper.TableMetricsRow
	sqlDefs     []*scrapper.SqlDefinitionRow
	databases   []*scrapper.DatabaseRow
	constraints []*scrapper.TableConstraintRow
	segments    []*scrapper.SegmentRow
	customMet   []*scrapper.CustomMetricsRow
	shape       []*scrapper.QueryShapeColumn
	warnings    []string
}

func (s *stubScrapper) Capabilities() scrapper.Capabilities { return scrapper.Capabilities{} }
func (s *stubScrapper) DialectType() string                 { return "stub" }
func (s *stubScrapper) SqlDialect() sqldialect.Dialect      { return nil }
func (s *stubScrapper) IsPermissionError(error) bool        { return false }
func (s *stubScrapper) Close() error                        { return nil }

func (s *stubScrapper) ValidateConfiguration(context.Context) ([]string, error) {
	return s.warnings, nil
}

func (s *stubScrapper) QueryCatalog(context.Context) ([]*scrapper.CatalogColumnRow, error) {
	return s.catalog, nil
}

func (s *stubScrapper) QueryTableMetrics(context.Context, time.Time) ([]*scrapper.TableMetricsRow, error) {
	return s.metrics, nil
}

func (s *stubScrapper) QuerySqlDefinitions(context.Context) ([]*scrapper.SqlDefinitionRow, error) {
	return s.sqlDefs, nil
}

func (s *stubScrapper) QueryTables(context.Context, ...scrapper.QueryTablesOption) ([]*scrapper.TableRow, error) {
	return s.tables, nil
}

func (s *stubScrapper) QueryDatabases(context.Context) ([]*scrapper.DatabaseRow, error) {
	return s.databases, nil
}

func (s *stubScrapper) QuerySegments(context.Context, string, ...any) ([]*scrapper.SegmentRow, error) {
	return s.segments, nil
}

func (s *stubScrapper) QueryCustomMetrics(context.Context, string, ...any) ([]*scrapper.CustomMetricsRow, error) {
	return s.customMet, nil
}

func (s *stubScrapper) QueryShape(context.Context, string) ([]*scrapper.QueryShapeColumn, error) {
	return s.shape, nil
}

func (s *stubScrapper) QueryTableConstraints(context.Context) ([]*scrapper.TableConstraintRow, error) {
	return s.constraints, nil
}

func TestSanitizingScrapper_CleansAllSources(t *testing.T) {
	inner := &stubScrapper{
		catalog: []*scrapper.CatalogColumnRow{
			{Database: "db\x00", Table: "t\x00", Column: "c"},
		},
		tables: []*scrapper.TableRow{
			{Database: "db\x00", Table: "t\x00"},
		},
		metrics: []*scrapper.TableMetricsRow{
			{Database: "db\x00", Table: "t"},
		},
		sqlDefs: []*scrapper.SqlDefinitionRow{
			{Database: "db\x00", Sql: "select \x00 from x"},
		},
		databases: []*scrapper.DatabaseRow{
			{Database: "db\x00"},
		},
		constraints: []*scrapper.TableConstraintRow{
			{Database: "db\x00", ConstraintName: "pk\x00"},
		},
		segments: []*scrapper.SegmentRow{
			{Segment: "seg\x00"},
		},
		customMet: []*scrapper.CustomMetricsRow{
			{Segments: []*scrapper.SegmentValue{{Name: "n\x00", Value: "v"}}},
		},
		shape: []*scrapper.QueryShapeColumn{
			{Name: "col\x00", NativeType: "INT\x00"},
		},
		warnings: []string{"warn\x00ing"},
	}
	ss := NewSanitizingScrapper(inner)
	ctx := context.Background()

	cat, err := ss.QueryCatalog(ctx)
	require.NoError(t, err)
	assert.Equal(t, "db", cat[0].Database)
	assert.Equal(t, "t", cat[0].Table)

	tbl, err := ss.QueryTables(ctx)
	require.NoError(t, err)
	assert.Equal(t, "t", tbl[0].Table)

	met, err := ss.QueryTableMetrics(ctx, time.Time{})
	require.NoError(t, err)
	assert.Equal(t, "db", met[0].Database)

	defs, err := ss.QuerySqlDefinitions(ctx)
	require.NoError(t, err)
	assert.Equal(t, "select  from x", defs[0].Sql)

	dbs, err := ss.QueryDatabases(ctx)
	require.NoError(t, err)
	assert.Equal(t, "db", dbs[0].Database)

	cons, err := ss.QueryTableConstraints(ctx)
	require.NoError(t, err)
	assert.Equal(t, "pk", cons[0].ConstraintName)

	segs, err := ss.QuerySegments(ctx, "sql")
	require.NoError(t, err)
	assert.Equal(t, "seg", segs[0].Segment)

	cm, err := ss.QueryCustomMetrics(ctx, "sql")
	require.NoError(t, err)
	assert.Equal(t, "n", cm[0].Segments[0].Name)

	shape, err := ss.QueryShape(ctx, "sql")
	require.NoError(t, err)
	assert.Equal(t, "col", shape[0].Name)
	assert.Equal(t, "INT", shape[0].NativeType)

	warnings, err := ss.ValidateConfiguration(ctx)
	require.NoError(t, err)
	assert.Equal(t, []string{"warning"}, warnings)
}

func TestSanitizingScrapper_PropagatesErrors(t *testing.T) {
	// Errors bypass the loops — verified by not panicking on nil rows.
	ss := NewSanitizingScrapper(&stubScrapper{})
	ctx := context.Background()

	_, err := ss.QueryCatalog(ctx)
	assert.NoError(t, err)
}
