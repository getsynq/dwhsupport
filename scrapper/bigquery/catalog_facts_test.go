package bigquery

import (
	"os"
	"testing"
	"time"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scope"
	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
	"github.com/stretchr/testify/suite"
)

// BigQueryCatalogFactsSuite checks column nullability, creation time and size
// on the products fixture. BigQuery names no table owner.
type BigQueryCatalogFactsSuite struct {
	scrappertest.CatalogFactsSuite
}

func TestBigQueryCatalogFactsSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping BigQuery catalog tests in CI")
	}
	suite.Run(t, new(BigQueryCatalogFactsSuite))
}

func (s *BigQueryCatalogFactsSuite) SetupSuite() {
	if os.Getenv("BIGQUERY_CREDENTIALS_FILE") == "" {
		s.T().Skip("BIGQUERY_CREDENTIALS_FILE env var not set")
	}
	sc, err := newBigQueryScrapperFromEnv(s.Ctx())
	if err != nil {
		s.T().Skipf("Could not connect to BigQuery: %v", err)
	}
	s.Scrapper = sc

	s.ColumnsTable = scrappertest.FixtureTable{Schema: "synq_dwhtesting", Table: "products"}
	s.NotNullColumn, s.NullableColumn = "id", "description"
	s.Table = s.ColumnsTable
	s.ExpectOwner, s.ExpectCreatedAt, s.ExpectSizeBytes = false, true, true

	s.DescribedSchema = scrappertest.FixtureSchema{Schema: "synq_dwhtesting_b"}
	s.SchemaDescription = "SYNQ dwhtesting secondary dataset — used for multi-dataset scope-scraping coverage"
}

// TestRepeatedColumnIsNotNullable pins what INFORMATION_SCHEMA.COLUMNS reports
// for an ARRAY column: NO, since an array is empty rather than NULL.
func (s *BigQueryCatalogFactsSuite) TestRepeatedColumnIsNotNullable() {
	rows, err := s.Scrapper.QueryCatalog(s.Ctx())
	s.Require().NoError(err)
	for _, row := range rows {
		if row.Schema == "synq_dwhtesting" && row.Table == "products" && row.Column == "tags" {
			s.Require().NotNil(row.IsNullable)
			s.False(*row.IsNullable)
			return
		}
	}
	s.Fail("synq_dwhtesting.products.tags not in the catalog")
}

// TestPrimaryKeyInKeyOrder reads the composite NOT ENFORCED primary key of
// order_items, declared as (order_id, item_id).
func (s *BigQueryCatalogFactsSuite) TestPrimaryKeyInKeyOrder() {
	ctx := scope.WithScope(s.Ctx(), &scope.ScopeFilter{
		Include: []scope.ScopeRule{{Schema: "synq_dwhtesting", Table: "order_items"}},
	})
	tables, err := s.Scrapper.QueryTables(ctx, scrapper.WithConstraints())
	s.Require().NoError(err)

	var keyColumns []string
	for _, table := range tables {
		if table.Schema != "synq_dwhtesting" || table.Table != "order_items" {
			continue
		}
		for _, c := range table.Constraints {
			if c.ConstraintType == scrapper.ConstraintTypePrimaryKey {
				s.Equal(int32(len(keyColumns)+1), c.ColumnPosition, c.ColumnName)
				s.Require().NotNil(c.IsEnforced)
				s.False(*c.IsEnforced)
				keyColumns = append(keyColumns, c.ColumnName)
			}
		}
	}
	s.Equal([]string{"order_id", "item_id"}, keyColumns)
}

// TestSnapshotAndCloneNameTheirBaseTable reads the seed's products_snapshot
// (CREATE SNAPSHOT TABLE ... CLONE products) and products_clone (CREATE TABLE
// ... CLONE products).
func (s *BigQueryCatalogFactsSuite) TestSnapshotAndCloneNameTheirBaseTable() {
	ctx := scope.WithScope(s.Ctx(), &scope.ScopeFilter{
		Include: []scope.ScopeRule{{Schema: "synq_dwhtesting"}},
	})
	tables, err := s.Scrapper.QueryTables(ctx)
	s.Require().NoError(err)

	copies := map[string]*scrapper.TableCopyOf{}
	project := ""
	for _, table := range tables {
		if table.Schema == "synq_dwhtesting" {
			copies[table.Table] = table.CopyOf
			project = table.Database
		}
	}
	s.Require().Contains(copies, "products")
	s.Nil(copies["products"])
	for name, kind := range map[string]scrapper.TableCopyKind{
		"products_snapshot": scrapper.TableCopySnapshot,
		"products_clone":    scrapper.TableCopyClone,
	} {
		c := copies[name]
		s.Require().NotNil(c, name)
		s.Equal(kind, c.Kind, name)
		s.Equal(scrapper.DwhFqn{DatabaseName: project, SchemaName: "synq_dwhtesting", ObjectName: "products"}, c.TableFqn(), name)
		s.Require().NotNil(c.At, name)
		s.Equal(time.UTC, c.At.Location(), name)
	}
}

// TestTableLabelReplacesTheDatasetLabel reads the tables labelled
// owner=analytics in a dataset labelled owner=data-eng.
func (s *BigQueryCatalogFactsSuite) TestTableLabelReplacesTheDatasetLabel() {
	ctx := scope.WithScope(s.Ctx(), &scope.ScopeFilter{
		Include: []scope.ScopeRule{{Schema: "synq_dwhtesting"}},
	})
	tables, err := s.Scrapper.QueryTables(ctx)
	s.Require().NoError(err)

	owners := map[string][]string{}
	for _, table := range tables {
		for _, tag := range table.Tags {
			if tag.TagName == "owner" {
				owners[table.Table] = append(owners[table.Table], tag.TagValue)
			}
		}
	}
	for _, name := range []string{"page_views", "events_hourly", "product_revenue_mv"} {
		s.Equal([]string{"analytics"}, owners[name], name)
	}
	s.Equal([]string{"data-eng"}, owners["products"], "products has no owner label of its own")
}

func (s *BigQueryCatalogFactsSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}
