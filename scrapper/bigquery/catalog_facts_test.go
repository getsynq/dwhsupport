package bigquery

import (
	"os"
	"testing"

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

func (s *BigQueryCatalogFactsSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}
