package reject

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
}

func (s *stubScrapper) Capabilities() scrapper.Capabilities { return scrapper.Capabilities{} }
func (s *stubScrapper) DialectType() string                 { return "stub" }
func (s *stubScrapper) SqlDialect() sqldialect.Dialect      { return nil }
func (s *stubScrapper) IsPermissionError(error) bool        { return false }
func (s *stubScrapper) Close() error                        { return nil }
func (s *stubScrapper) ValidateConfiguration(context.Context) ([]string, error) {
	return nil, nil
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

func TestRejectingScrapper_QueryCatalog_DropsBadRowsAndNestedTags(t *testing.T) {
	comment := "desc\x00" // content NUL — content is NOT identity, row stays
	inner := &stubScrapper{
		catalog: []*scrapper.CatalogColumnRow{
			{Database: "good_db", Schema: "s", Table: "t", Column: "c", Comment: &comment,
				ColumnTags: []*scrapper.Tag{
					{TagName: "ok", TagValue: "v"},
					{TagName: "bad\x00", TagValue: "v"},
				},
				FieldSchemas: []*scrapper.SchemaColumnField{
					{Name: "ok_field"},
					{Name: "bad\x00field"},
				},
			},
			{Database: "bad\x00db", Schema: "s", Table: "t", Column: "c"},
		},
	}
	ss := NewRejectingScrapper(inner)
	rows, err := ss.QueryCatalog(context.Background())
	require.NoError(t, err)
	require.Len(t, rows, 1, "row with NUL in Database must be dropped")
	assert.Equal(t, "good_db", rows[0].Database)
	require.Len(t, rows[0].ColumnTags, 1)
	assert.Equal(t, "ok", rows[0].ColumnTags[0].TagName)
	require.Len(t, rows[0].FieldSchemas, 1)
	assert.Equal(t, "ok_field", rows[0].FieldSchemas[0].Name)
	// Content field passes through untouched — caller composes with sanitize to clean it.
	require.NotNil(t, rows[0].Comment)
	assert.Equal(t, "desc\x00", *rows[0].Comment)
}

func TestRejectingScrapper_QueryTables_FiltersNested(t *testing.T) {
	inner := &stubScrapper{
		tables: []*scrapper.TableRow{
			{
				Database: "db", Schema: "s", Table: "t",
				Tags:        []*scrapper.Tag{{TagName: "k", TagValue: "v"}, {TagName: "k\x00", TagValue: "v"}},
				Annotations: []*scrapper.Annotation{{AnnotationName: "ok", AnnotationValue: "v"}, nil},
				Constraints: []*scrapper.TableConstraintRow{
					{Instance: "i", Database: "db", Schema: "s", Table: "t", ConstraintName: "pk", ColumnName: "id"},
					{Instance: "i", Database: "db", Schema: "s", Table: "t\x00", ConstraintName: "pk", ColumnName: "id"},
				},
			},
			{Database: "db\x00"},
		},
	}
	ss := NewRejectingScrapper(inner)
	rows, err := ss.QueryTables(context.Background())
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Len(t, rows[0].Tags, 1)
	require.Len(t, rows[0].Annotations, 1)
	require.Len(t, rows[0].Constraints, 1)
	assert.Equal(t, "pk", rows[0].Constraints[0].ConstraintName)
}

func TestRejectingScrapper_SimpleRowTypes(t *testing.T) {
	inner := &stubScrapper{
		metrics:     []*scrapper.TableMetricsRow{{Database: "ok", Table: "t"}, {Database: "bad\x00", Table: "t"}},
		sqlDefs:     []*scrapper.SqlDefinitionRow{{Database: "ok"}, {Database: "bad\x00"}},
		databases:   []*scrapper.DatabaseRow{{Database: "ok"}, {Database: "bad\x00"}},
		constraints: []*scrapper.TableConstraintRow{{Database: "ok", ConstraintName: "pk", ColumnName: "id"}, {Database: "bad\x00"}},
		segments:    []*scrapper.SegmentRow{{Segment: "ok"}, {Segment: "bad\x00"}},
		customMet: []*scrapper.CustomMetricsRow{
			{Segments: []*scrapper.SegmentValue{{Name: "ok"}}},
			{Segments: []*scrapper.SegmentValue{{Name: "bad\x00"}}},
		},
		shape: []*scrapper.QueryShapeColumn{{Name: "ok"}, {Name: "bad\x00"}},
	}
	ss := NewRejectingScrapper(inner)
	ctx := context.Background()

	met, err := ss.QueryTableMetrics(ctx, time.Time{})
	require.NoError(t, err)
	assert.Len(t, met, 1)

	defs, err := ss.QuerySqlDefinitions(ctx)
	require.NoError(t, err)
	assert.Len(t, defs, 1)

	dbs, err := ss.QueryDatabases(ctx)
	require.NoError(t, err)
	assert.Len(t, dbs, 1)

	cons, err := ss.QueryTableConstraints(ctx)
	require.NoError(t, err)
	assert.Len(t, cons, 1)

	segs, err := ss.QuerySegments(ctx, "sql")
	require.NoError(t, err)
	assert.Len(t, segs, 1)

	cm, err := ss.QueryCustomMetrics(ctx, "sql")
	require.NoError(t, err)
	assert.Len(t, cm, 1)

	shape, err := ss.QueryShape(ctx, "sql")
	require.NoError(t, err)
	assert.Len(t, shape, 1)
}
