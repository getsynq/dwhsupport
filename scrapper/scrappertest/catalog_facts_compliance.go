package scrappertest

import (
	"context"
	"strings"
	"time"

	"github.com/getsynq/dwhsupport/exec/querycontext"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scope"
	"github.com/stretchr/testify/suite"
)

// FixtureTable names a table of a warehouse's dwhtesting seed as the catalog
// spells it, matched case-insensitively. An empty Database or Schema matches
// any.
type FixtureTable struct {
	Database, Schema, Table string
}

func (f FixtureTable) ctx(ctx context.Context) context.Context {
	return scope.WithScope(querycontext.WithQueryContext(ctx, complianceQueryContext), &scope.ScopeFilter{
		Include: []scope.ScopeRule{{Database: f.Database, Schema: f.Schema, Table: f.Table}},
	})
}

func (f FixtureTable) matches(fqn scrapper.DwhFqn) bool {
	return (f.Database == "" || strings.EqualFold(fqn.DatabaseName, f.Database)) &&
		(f.Schema == "" || strings.EqualFold(fqn.SchemaName, f.Schema)) &&
		strings.EqualFold(fqn.ObjectName, f.Table)
}

func (f FixtureTable) String() string {
	return f.Database + "." + f.Schema + "." + f.Table
}

// FixtureSchema names a schema of a warehouse's dwhtesting seed as the catalog
// spells it, matched case-insensitively. An empty Database matches any.
type FixtureSchema struct {
	Database, Schema string
}

func (f FixtureSchema) matches(row *scrapper.SchemaRow) bool {
	return (f.Database == "" || strings.EqualFold(row.Database, f.Database)) && strings.EqualFold(row.Schema, f.Schema)
}

func (f FixtureSchema) String() string {
	return f.Database + "." + f.Schema
}

// CatalogFactsSuite checks the facts a catalog reads about a table beyond its
// columns' types: whether a column accepts NULL, who owns the table, when it
// was created and how many bytes it holds, and the description of a schema.
//
// Embed it in a warehouse test and set, in SetupSuite, Scrapper, the
// ColumnsTable with one column declared NOT NULL and one that accepts NULL
// (left empty when the warehouse's seed has no such table), and the Table
// whose owner, creation time and size are read (often the same table). A fact the warehouse does not report must stay nil, so an Expect
// left false asserts nil rather than skipping. DescribedSchema is a schema the
// seed comments and SchemaDescription that comment, left empty when the
// warehouse's seed comments no schema.
type CatalogFactsSuite struct {
	suite.Suite
	Scrapper scrapper.Scrapper

	ColumnsTable   FixtureTable
	NotNullColumn  string
	NullableColumn string

	Table           FixtureTable
	ExpectOwner     bool
	ExpectCreatedAt bool
	// ExpectSizeBytes asserts QueryTableMetrics reports a size above zero for
	// Table, which must hold rows.
	ExpectSizeBytes bool

	DescribedSchema   FixtureSchema
	SchemaDescription string
}

func (s *CatalogFactsSuite) Ctx() context.Context {
	return context.Background()
}

func (s *CatalogFactsSuite) TestCatalogFacts_ColumnNullable() {
	if s.ColumnsTable.Table == "" {
		s.T().Skip("no fixture table with a NOT NULL column")
	}
	s.Require().NotEmpty(s.NotNullColumn, "set NotNullColumn in SetupSuite")
	s.Require().NotEmpty(s.NullableColumn, "set NullableColumn in SetupSuite")
	rows, err := s.Scrapper.QueryCatalog(s.ColumnsTable.ctx(s.Ctx()))
	s.Require().NoError(err)

	columns := map[string]*scrapper.CatalogColumnRow{}
	for _, row := range rows {
		if s.ColumnsTable.matches(row.TableFqn()) {
			columns[strings.ToLower(row.Column)] = row
		}
	}
	s.Require().NotEmpty(columns, "fixture table %s not in the catalog", s.ColumnsTable)

	for column, want := range map[string]bool{s.NotNullColumn: false, s.NullableColumn: true} {
		row, ok := columns[strings.ToLower(column)]
		if !s.Truef(ok, "column %s not in the catalog of %s", column, s.ColumnsTable) {
			continue
		}
		if s.NotNilf(row.IsNullable, "%s (%s) is_nullable", column, row.Type) {
			s.Equalf(want, *row.IsNullable, "%s (%s) is_nullable", column, row.Type)
		}
	}
}

func (s *CatalogFactsSuite) TestCatalogFacts_TableOwnerAndCreatedAt() {
	rows, err := s.Scrapper.QueryTables(s.Table.ctx(s.Ctx()))
	s.Require().NoError(err)

	var table *scrapper.TableRow
	for _, row := range rows {
		if s.Table.matches(row.TableFqn()) {
			table = row
		}
	}
	s.Require().NotNil(table, "fixture table %s not listed", s.Table)

	if s.ExpectOwner {
		if s.NotNil(table.Owner, "owner") {
			s.NotEmpty(*table.Owner, "owner")
		}
	} else {
		s.Nil(table.Owner, "owner of a warehouse that reports none")
	}

	if !s.ExpectCreatedAt {
		s.Nil(table.CreatedAt, "created_at of a warehouse that reports none")
		return
	}
	if s.NotNil(table.CreatedAt, "created_at") {
		s.Equal(time.UTC, table.CreatedAt.Location(), "created_at is UTC")
		s.True(table.CreatedAt.Before(time.Now()), "created_at %s is in the past", table.CreatedAt)
		s.True(table.CreatedAt.After(time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)), "created_at %s is a real time", table.CreatedAt)
	}
}

func (s *CatalogFactsSuite) TestCatalogFacts_SizeBytes() {
	if !s.ExpectSizeBytes {
		s.T().Skip("the warehouse reports no table size")
	}
	rows, err := s.Scrapper.QueryTableMetrics(s.Table.ctx(s.Ctx()), time.Time{})
	s.Require().NoError(err)

	var found bool
	for _, row := range rows {
		if !s.Table.matches(row.TableFqn()) {
			continue
		}
		found = true
		if s.NotNil(row.SizeBytes, "size_bytes") {
			s.Positive(*row.SizeBytes, "size_bytes")
		}
	}
	s.True(found, "fixture table %s has no metrics", s.Table)
}

func (s *CatalogFactsSuite) TestCatalogFacts_SchemaDescription() {
	if s.DescribedSchema.Schema == "" {
		s.T().Skip("no fixture schema with a description")
	}
	rows, err := s.Scrapper.QuerySchemas(s.Ctx())
	s.Require().NoError(err)

	var schema *scrapper.SchemaRow
	for _, row := range rows {
		if s.DescribedSchema.matches(row) {
			schema = row
		}
	}
	s.Require().NotNil(schema, "fixture schema %s not listed", s.DescribedSchema)
	if s.NotNil(schema.Description, "description of %s", s.DescribedSchema) {
		s.Equal(s.SchemaDescription, *schema.Description, "description of %s", s.DescribedSchema)
	}
}
