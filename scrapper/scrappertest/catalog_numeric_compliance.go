package scrappertest

import (
	"context"
	"strings"

	"github.com/getsynq/dwhsupport/exec/querycontext"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scope"
	"github.com/stretchr/testify/suite"
)

// CatalogNumericColumn is what QueryCatalog must report for one column of a
// fixture table: its numeric category, and its precision and scale, which
// stay nil when the warehouse does not report a size.
type CatalogNumericColumn struct {
	Column    string
	Category  scrapper.NumericCategory
	Precision *int64
	Scale     *int64
}

// Sized is a precision and scale for CatalogNumericColumn.
func Sized(precision, scale int64) (*int64, *int64) {
	return &precision, &scale
}

// CatalogNumericSuite checks the precision, scale and numeric category
// QueryCatalog reports for the columns of one fixture table the warehouse's
// dwhtesting seed declares, so a catalog query that loses the size of a
// DECIMAL(10,2), or reports a size for a column declared without one, fails.
//
// Embed it in a warehouse test, set Scrapper, the fixture table (as the
// catalog spells it, matched case-insensitively; an empty Database or Schema
// matches any) and Want in SetupSuite.
type CatalogNumericSuite struct {
	suite.Suite
	Scrapper scrapper.Scrapper

	Database, Schema, Table string
	Want                    []CatalogNumericColumn
}

func (s *CatalogNumericSuite) Ctx() context.Context {
	return context.Background()
}

func (s *CatalogNumericSuite) TestCatalogNumericSize() {
	s.Require().NotEmpty(s.Want, "set Want in SetupSuite")
	ctx := scope.WithScope(querycontext.WithQueryContext(s.Ctx(), complianceQueryContext), &scope.ScopeFilter{
		Include: []scope.ScopeRule{{Database: s.Database, Schema: s.Schema, Table: s.Table}},
	})
	rows, err := s.Scrapper.QueryCatalog(ctx)
	s.Require().NoError(err)

	columns := map[string]*scrapper.CatalogColumnRow{}
	for _, row := range rows {
		if (s.Schema == "" || strings.EqualFold(row.Schema, s.Schema)) && strings.EqualFold(row.Table, s.Table) {
			columns[strings.ToLower(row.Column)] = row
		}
	}
	s.Require().NotEmpty(columns, "fixture table %s.%s.%s not in the catalog", s.Database, s.Schema, s.Table)

	for _, want := range s.Want {
		row, ok := columns[strings.ToLower(want.Column)]
		if !s.Truef(ok, "column %s not in the catalog of %s.%s", want.Column, s.Schema, s.Table) {
			continue
		}
		s.Equalf(want.Category, row.NumericCategory, "%s (%s) category", want.Column, row.Type)
		s.Equalf(want.Precision, row.Precision, "%s (%s) precision", want.Column, row.Type)
		s.Equalf(want.Scale, row.Scale, "%s (%s) scale", want.Column, row.Type)
	}
}
