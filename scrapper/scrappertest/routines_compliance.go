package scrappertest

import (
	"context"
	"strings"

	"github.com/getsynq/dwhsupport/exec/querycontext"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scope"
	"github.com/stretchr/testify/suite"
)

// FixtureRoutine names a routine of a warehouse's dwhtesting seed as the
// catalog spells it, matched case-insensitively. An empty Database matches any.
// DefinitionContains is text its definition must hold, matched
// case-insensitively, such as the table it reads, so a definition that lost its
// body fails.
type FixtureRoutine struct {
	Database, Schema, Name string
	Kind                   scrapper.RoutineKind
	DefinitionContains     string
}

func (f FixtureRoutine) matches(row *scrapper.RoutineRow) bool {
	return (f.Database == "" || strings.EqualFold(row.Database, f.Database)) &&
		strings.EqualFold(row.Schema, f.Schema) &&
		strings.EqualFold(row.Name, f.Name)
}

func (f FixtureRoutine) String() string {
	return f.Database + "." + f.Schema + "." + f.Name
}

// RoutinesSuite checks QueryRoutines lists the procedures and functions of a
// warehouse's dwhtesting seed with the statement that creates each, and that a
// scope narrows the listing like it narrows tables.
//
// Embed it in a warehouse test and set Scrapper and Expect, at least one
// routine, in SetupSuite.
type RoutinesSuite struct {
	suite.Suite
	Scrapper scrapper.Scrapper
	Expect   []FixtureRoutine
}

func (s *RoutinesSuite) Ctx() context.Context {
	return querycontext.WithQueryContext(context.Background(), complianceQueryContext)
}

func (s *RoutinesSuite) TestRoutines_SeededAreListed() {
	s.Require().NotEmpty(s.Expect, "set Expect to the seed's routines")
	rows, err := s.Scrapper.QueryRoutines(s.Ctx())
	s.Require().NoError(err)

	for _, expected := range s.Expect {
		row := findRoutine(rows, expected)
		if !s.NotNilf(row, "routine %s not listed", expected) {
			continue
		}
		s.NotEmptyf(row.Instance+row.Database, "routine %s has neither instance nor database", expected)
		s.Equalf(expected.Kind, row.Kind, "routine %s kind", expected)
		s.NotEmptyf(row.RoutineType, "routine %s type", expected)
		if s.NotNilf(row.Definition, "routine %s has no definition", expected) {
			s.Containsf(strings.ToLower(*row.Definition), strings.ToLower(expected.DefinitionContains),
				"routine %s definition", expected)
		}
	}
}

func (s *RoutinesSuite) TestRoutines_ScopeNarrows() {
	s.Require().NotEmpty(s.Expect, "set Expect to the seed's routines")
	target := s.Expect[0]

	ctx := scope.WithScope(s.Ctx(), &scope.ScopeFilter{
		Include: []scope.ScopeRule{{Database: target.Database, Schema: target.Schema, Table: target.Name}},
	})
	rows, err := s.Scrapper.QueryRoutines(ctx)
	s.Require().NoError(err)
	s.Require().NotNil(findRoutine(rows, target), "routine %s not listed in its own scope", target)
	for _, row := range rows {
		s.Truef(strings.EqualFold(row.Schema, target.Schema) && strings.EqualFold(row.Name, target.Name),
			"routine %s.%s listed outside the scope %s", row.Schema, row.Name, target)
	}

	excluded := scope.WithScope(s.Ctx(), &scope.ScopeFilter{
		Exclude: []scope.ScopeRule{{Database: target.Database, Schema: target.Schema}},
	})
	rows, err = s.Scrapper.QueryRoutines(excluded)
	s.Require().NoError(err)
	for _, row := range rows {
		s.Falsef(strings.EqualFold(row.Schema, target.Schema) && (target.Database == "" || strings.EqualFold(row.Database, target.Database)),
			"routine %s.%s listed from the excluded schema", row.Schema, row.Name)
	}
}

func findRoutine(rows []*scrapper.RoutineRow, f FixtureRoutine) *scrapper.RoutineRow {
	for _, row := range rows {
		if f.matches(row) {
			return row
		}
	}
	return nil
}
