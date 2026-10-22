package scrappertest

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/stretchr/testify/suite"
)

// Constraint is a key, index, check or partitioning of a warehouse's
// dwhtesting seed: the table it is on, its type (scrapper.ConstraintType*), its
// columns in key order (none for a CHECK) and its ConstraintExpression, which
// is compared exactly, so an empty one must come back empty. Names are matched
// case-insensitively, as the catalog spells them.
//
// IsEnforced, IsRely and IndexType are compared only when an expectation sets
// them: nil and empty mean the test does not care, not that the scrapper must
// leave them unset. UnsetEnforced asks for an IsEnforced that is nil.
type Constraint struct {
	Table         FixtureTable
	Type          string
	Columns       []string
	Expression    string
	IsEnforced    *bool
	UnsetEnforced bool
	IsRely        *bool
	IndexType     string
}

func (c Constraint) String() string {
	return fmt.Sprintf(
		"%s %s %v %q enforced=%s rely=%s index_type=%q",
		c.Table, c.Type, c.Columns, c.Expression, boolPtrString(c.IsEnforced), boolPtrString(c.IsRely), c.IndexType,
	)
}

func boolPtrString(v *bool) string {
	if v == nil {
		return "nil"
	}
	return fmt.Sprint(*v)
}

// matches is whether a reported constraint is the one want expects.
func (c Constraint) matches(want Constraint) bool {
	if c.Type != want.Type || !equalFold(c.Columns, want.Columns) || c.Expression != want.Expression {
		return false
	}
	if want.IsEnforced != nil && (c.IsEnforced == nil || *c.IsEnforced != *want.IsEnforced) {
		return false
	}
	if want.UnsetEnforced && c.IsEnforced != nil {
		return false
	}
	if want.IsRely != nil && (c.IsRely == nil || *c.IsRely != *want.IsRely) {
		return false
	}
	return want.IndexType == "" || strings.EqualFold(c.IndexType, want.IndexType)
}

// TableConstraintsSuite checks that a scrapper reports the constraints the
// seed declares, and only constraints of the tables it lists.
//
// Embed it in a warehouse test and set Scrapper and Expect in SetupSuite. A
// scrapper whose capabilities say constraints come with QueryTables is read
// that way, every other one through QueryTableConstraints.
type TableConstraintsSuite struct {
	suite.Suite
	Scrapper scrapper.Scrapper
	// Expect are constraints the scrapper must report, each as one constraint
	// (rows sharing a name) of that type, columns and expression.
	Expect []Constraint
	// Unexpected are constraints the scrapper must not report, matched by
	// table, type and columns, such as the index backing a primary key
	// reported again as an index.
	Unexpected []Constraint
}

func (s *TableConstraintsSuite) Ctx() context.Context {
	return context.Background()
}

// queryConstraints reads a scrapper's constraints the way its capabilities say
// they come.
func queryConstraints(ctx context.Context, sc scrapper.Scrapper) ([]*scrapper.TableConstraintRow, error) {
	if !sc.Capabilities().ConstraintsViaQueryTables {
		return sc.QueryTableConstraints(ctx)
	}
	tables, err := sc.QueryTables(ctx, scrapper.WithConstraints())
	if err != nil {
		return nil, err
	}
	var rows []*scrapper.TableConstraintRow
	for _, t := range tables {
		rows = append(rows, t.Constraints...)
	}
	return rows, nil
}

// constraintsOf groups the constraint rows of a table into constraints, by
// type and name, each with its rows in key order.
func (s *TableConstraintsSuite) constraintsOf(table FixtureTable) []Constraint {
	rows, err := queryConstraints(table.ctx(s.Ctx()), s.Scrapper)
	s.Require().NoError(err)

	type key struct{ constraintType, name string }
	grouped := map[key][]*scrapper.TableConstraintRow{}
	var order []key
	for _, row := range rows {
		if !table.matches(row.TableFqn()) {
			continue
		}
		k := key{row.ConstraintType, row.ConstraintName}
		if _, ok := grouped[k]; !ok {
			order = append(order, k)
		}
		grouped[k] = append(grouped[k], row)
	}

	var constraints []Constraint
	for _, k := range order {
		group := grouped[k]
		sort.SliceStable(group, func(i, j int) bool { return group[i].ColumnPosition < group[j].ColumnPosition })
		c := Constraint{
			Table:      table,
			Type:       k.constraintType,
			Expression: group[0].ConstraintExpression,
			IsEnforced: group[0].IsEnforced,
			IsRely:     group[0].IsRely,
			IndexType:  group[0].IndexType,
		}
		for _, row := range group {
			if row.ColumnName != "" {
				c.Columns = append(c.Columns, row.ColumnName)
			}
		}
		constraints = append(constraints, c)
	}
	return constraints
}

func (s *TableConstraintsSuite) TestTableConstraints_AreReported() {
	s.Require().NotEmpty(s.Expect, "set Expect in SetupSuite")
	for _, want := range s.Expect {
		got := s.constraintsOf(want.Table)
		var found bool
		for _, c := range got {
			if c.matches(want) {
				found = true
				break
			}
		}
		s.Truef(found, "want %s, got %v", want, got)
	}
}

func (s *TableConstraintsSuite) TestTableConstraints_UnexpectedAreNotReported() {
	for _, unwanted := range s.Unexpected {
		for _, c := range s.constraintsOf(unwanted.Table) {
			s.Falsef(c.Type == unwanted.Type && equalFold(c.Columns, unwanted.Columns), "reported %s", c)
		}
	}
}

// TestTableConstraints_NameListedTables reads every constraint and every table
// in the scrapper's configured scope, and checks that each constraint is on a
// table QueryTables lists: never on an index, a system table or anything else
// the catalog leaves out.
func (s *TableConstraintsSuite) TestTableConstraints_NameListedTables() {
	if s.Scrapper.Capabilities().ConstraintsViaQueryTables {
		s.T().Skip("constraints come with QueryTables, so every one is on a listed table")
	}
	ctx := s.Ctx()
	tables, err := s.Scrapper.QueryTables(ctx)
	s.Require().NoError(err)
	listed := map[string]bool{}
	for _, t := range tables {
		listed[strings.ToLower(t.Schema+"."+t.Table)] = true
	}

	rows, err := s.Scrapper.QueryTableConstraints(ctx)
	s.Require().NoError(err)
	unlisted := map[string]bool{}
	for _, row := range rows {
		name := strings.ToLower(row.Schema + "." + row.Table)
		if !listed[name] {
			unlisted[name] = true
		}
	}
	s.Emptyf(unlisted, "constraints on %d tables QueryTables does not list", len(unlisted))
}
