package scrappertest

import (
	"context"
	"sort"
	"strings"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/stretchr/testify/suite"
)

// ForeignKey is a foreign key of a warehouse's dwhtesting seed: the table that
// declares it, its columns, and the table and columns it references. Names are
// matched case-insensitively, as the catalog spells them.
type ForeignKey struct {
	Table             FixtureTable
	Columns           []string
	Referenced        FixtureTable
	ReferencedColumns []string
}

// ForeignKeysSuite checks that a scrapper reports each foreign key of the
// seed as a FOREIGN KEY, with its columns in key order and, on each column's
// row, the referenced table and column.
//
// Embed it in a warehouse test and set Scrapper and ForeignKeys in SetupSuite.
// A scrapper whose capabilities say constraints come with QueryTables is read
// that way, every other one through QueryTableConstraints.
type ForeignKeysSuite struct {
	suite.Suite
	Scrapper    scrapper.Scrapper
	ForeignKeys []ForeignKey
	// NeverReferenced are tables no foreign key of the ForeignKeys tables may
	// name, such as the partitions of a referenced partitioned table.
	NeverReferenced []FixtureTable
}

func (s *ForeignKeysSuite) Ctx() context.Context {
	return context.Background()
}

func (s *ForeignKeysSuite) constraintsOf(table FixtureTable) []*scrapper.TableConstraintRow {
	ctx := table.ctx(s.Ctx())
	var rows []*scrapper.TableConstraintRow
	if s.Scrapper.Capabilities().ConstraintsViaQueryTables {
		tables, err := s.Scrapper.QueryTables(ctx, scrapper.WithConstraints())
		s.Require().NoError(err)
		for _, t := range tables {
			rows = append(rows, t.Constraints...)
		}
	} else {
		var err error
		rows, err = s.Scrapper.QueryTableConstraints(ctx)
		s.Require().NoError(err)
	}
	var matching []*scrapper.TableConstraintRow
	for _, row := range rows {
		if table.matches(row.TableFqn()) {
			matching = append(matching, row)
		}
	}
	return matching
}

func (s *ForeignKeysSuite) TestForeignKeys_ReferenceTheirTable() {
	s.Require().NotEmpty(s.ForeignKeys, "set ForeignKeys in SetupSuite")
	for _, want := range s.ForeignKeys {
		rows := s.constraintsOf(want.Table)

		// One foreign key per declared column set, so a table with two foreign
		// keys is checked key by key.
		byName := map[string][]*scrapper.TableConstraintRow{}
		for _, row := range rows {
			if row.ConstraintType == scrapper.ConstraintTypeForeignKey {
				byName[row.ConstraintName] = append(byName[row.ConstraintName], row)
			}
		}

		for name, key := range byName {
			for _, row := range key {
				referenced, _ := row.ReferencedTableFqn()
				for _, never := range s.NeverReferenced {
					s.Falsef(never.matches(referenced), "%s %s references %s", want.Table, name, never)
				}
			}
		}

		var found bool
		for name, key := range byName {
			sort.Slice(key, func(i, j int) bool { return key[i].ColumnPosition < key[j].ColumnPosition })
			var columns, referencedColumns []string
			for _, row := range key {
				columns = append(columns, strings.ToLower(row.ColumnName))
				referencedColumns = append(referencedColumns, strings.ToLower(row.ReferencedColumnName))
			}
			if !equalFold(columns, want.Columns) {
				continue
			}
			found = true
			s.Equalf(lower(want.ReferencedColumns), referencedColumns, "%s %s referenced columns", want.Table, name)
			for _, row := range key {
				referenced, ok := row.ReferencedTableFqn()
				if s.Truef(ok, "%s %s names no referenced table", want.Table, name) {
					s.Truef(want.Referenced.matches(referenced), "%s %s references %+v, want %s", want.Table, name, referenced, want.Referenced)
				}
				s.NotEmptyf(row.ReferencedDatabase, "%s %s referenced database", want.Table, name)
			}
		}
		s.Truef(found, "%s has no FOREIGN KEY on %v among %d constraint rows", want.Table, want.Columns, len(rows))
	}
}

func equalFold(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if !strings.EqualFold(got[i], want[i]) {
			return false
		}
	}
	return true
}

func lower(values []string) []string {
	out := make([]string, len(values))
	for i, v := range values {
		out[i] = strings.ToLower(v)
	}
	return out
}
