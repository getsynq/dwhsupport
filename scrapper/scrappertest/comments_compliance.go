package scrappertest

import (
	"context"
	"strings"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/stretchr/testify/suite"
)

// CommentsSuite checks that the comments written in the warehouse reach the
// rows a scrapper returns: a table's comment as the TableRow description and,
// where the catalog query reads it, as CatalogColumnRow.TableComment, and each
// column's comment as CatalogColumnRow.Comment. A view without a comment must
// come back without one, because MySQL and MariaDB report the comment of every
// view as the text "VIEW".
//
// Embed it in a warehouse test and set, in SetupSuite, Scrapper, the commented
// Table with its TableComment and ColumnComments (leave Table empty when the
// warehouse's seed has no comments), and View, a view with no comment (left
// empty when the seed has none). CatalogTableComments says whether QueryCatalog
// reads the table comment at all; a warehouse whose catalog query does not
// read it must leave TableComment empty.
type CommentsSuite struct {
	suite.Suite
	Scrapper scrapper.Scrapper

	Table          FixtureTable
	TableComment   string
	ColumnComments map[string]string
	// UncommentedColumn is a column of Table without a comment, whose Comment
	// must be empty.
	UncommentedColumn string

	View FixtureTable

	CatalogTableComments bool
}

func (s *CommentsSuite) Ctx() context.Context {
	return context.Background()
}

func (s *CommentsSuite) TestComments_TableDescription() {
	if s.Table.Table == "" {
		s.T().Skip("no fixture table with a comment")
	}
	table := s.tableRow(s.Table)
	s.Equal(s.TableComment, deref(table.Description), "description of %s", s.Table)
}

func (s *CommentsSuite) TestComments_ColumnComments() {
	if s.Table.Table == "" {
		s.T().Skip("no fixture table with a comment")
	}
	columns := s.catalogColumns(s.Table)
	for column, want := range s.ColumnComments {
		row, ok := columns[strings.ToLower(column)]
		if s.Truef(ok, "column %s not in the catalog of %s", column, s.Table) {
			s.Equalf(want, deref(row.Comment), "comment of %s.%s", s.Table, column)
		}
	}
	if s.UncommentedColumn != "" {
		row, ok := columns[strings.ToLower(s.UncommentedColumn)]
		if s.Truef(ok, "column %s not in the catalog of %s", s.UncommentedColumn, s.Table) {
			s.Emptyf(deref(row.Comment), "comment of %s.%s", s.Table, s.UncommentedColumn)
		}
	}
	if !s.CatalogTableComments {
		return
	}
	for column, row := range columns {
		s.Equalf(s.TableComment, deref(row.TableComment), "table comment on column %s of %s", column, s.Table)
	}
}

func (s *CommentsSuite) TestComments_ViewHasNoComment() {
	if s.View.Table == "" {
		s.T().Skip("no fixture view")
	}
	view := s.tableRow(s.View)
	s.Empty(deref(view.Description), "description of view %s", s.View)
	for column, row := range s.catalogColumns(s.View) {
		s.Emptyf(deref(row.TableComment), "table comment on column %s of view %s", column, s.View)
	}
}

func (s *CommentsSuite) tableRow(fixture FixtureTable) *scrapper.TableRow {
	rows, err := s.Scrapper.QueryTables(fixture.ctx(s.Ctx()))
	s.Require().NoError(err)
	for _, row := range rows {
		if fixture.matches(row.TableFqn()) {
			return row
		}
	}
	s.Require().Failf("fixture not listed", "%s is not in QueryTables", fixture)
	return nil
}

func (s *CommentsSuite) catalogColumns(fixture FixtureTable) map[string]*scrapper.CatalogColumnRow {
	rows, err := s.Scrapper.QueryCatalog(fixture.ctx(s.Ctx()))
	s.Require().NoError(err)
	columns := map[string]*scrapper.CatalogColumnRow{}
	for _, row := range rows {
		if fixture.matches(row.TableFqn()) {
			columns[strings.ToLower(row.Column)] = row
		}
	}
	s.Require().NotEmpty(columns, "fixture %s not in the catalog", fixture)
	return columns
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
