package sqldialect

import (
	"os"
	"testing"

	"github.com/gkampitakis/go-snaps/snaps"
	"github.com/stretchr/testify/suite"
)

type BaseSuite struct {
	suite.Suite
}

func TestBaseSuite(t *testing.T) {
	suite.Run(t, new(BaseSuite))
}

func (s *BaseSuite) TestCte() {
	for _, dialect := range DialectsToTest() {
		s.Run(dialect.Name, func() {
			cteSelect := NewSelect().Cols(Star()).
				From(TableFqn("proj", "default", "users")).
				OrderBy(Desc(Identifier("created_at"))).
				WithLimit(Limit(Int64(10)))
			cteFqn := CteFqn("cte")

			cte := NewSelect().Cte(cteFqn, cteSelect).From(cteFqn).Cols(Star())
			sql, err := cte.ToSql(dialect.Dialect)
			s.Require().NoError(err)
			snaps.WithConfig(snaps.Dir("Cte"), snaps.Filename(dialect.Name)).MatchSnapshot(s.T(), sql)
		})
	}
}

func (s *BaseSuite) TestTableFn() {
	for _, dialect := range DialectsToTest() {
		s.Run(dialect.Name, func() {
			sel := NewSelect().Cols(Star()).
				From(TableFn(
					"SNOWFLAKE.CORE.GET_LINEAGE",
					String("my_database.sch.table_a"),
					String("TABLE"),
					String("DOWNSTREAM"),
					Int64(2),
				)).
				OrderBy(Desc(Identifier("created_at"))).
				WithLimit(Limit(Int64(10)))

			sql, err := sel.ToSql(dialect.Dialect)
			s.Require().NoError(err)
			snaps.WithConfig(snaps.Dir("TableFn"), snaps.Filename(dialect.Name)).MatchSnapshot(s.T(), sql)
		})
	}
}

func (s *BaseSuite) TestPqQuoteIdentifierIfUpper() {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"empty string", "", ""},
		{"lowercase identifier", "user", "user"},
		{"mixed case identifier", "User", `"User"`},
		{"uppercase identifier", "USER", `"USER"`},
		{"identifier with numbers", "user123", "user123"},
		{"identifier with underscores", "user_name", "user_name"},
		{"mixed case with underscores", "User_Name", `"User_Name"`},
		{"single uppercase letter", "A", `"A"`},
		{"single lowercase letter", "a", "a"},
		{"special characters", "user@domain", "user@domain"},
		{"camelCase", "firstName", `"firstName"`},
		{"SCREAMING_CASE", "FIRST_NAME", `"FIRST_NAME"`},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			result := PqQuoteIdentifierIfUpper(tt.input)
			s.Equal(tt.expected, result)
		})
	}
}

func (s *BaseSuite) TestIsLower() {
	tests := []struct {
		name     string
		input    string
		expected bool
	}{
		{"empty string", "", true},
		{"lowercase letters", "hello", true},
		{"uppercase letters", "HELLO", false},
		{"mixed case", "Hello", false},
		{"lowercase with numbers", "hello123", true},
		{"uppercase with numbers", "HELLO123", false},
		{"mixed case with numbers", "Hello123", false},
		{"lowercase with underscores", "hello_world", true},
		{"uppercase with underscores", "HELLO_WORLD", false},
		{"mixed case with underscores", "Hello_World", false},
		{"numbers only", "123", true},
		{"special characters only", "_@#$", true},
		{"lowercase with special chars", "hello@world", true},
		{"uppercase with special chars", "HELLO@WORLD", false},
		{"single lowercase letter", "a", true},
		{"single uppercase letter", "A", false},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			result := IsLower(tt.input)
			s.Equal(tt.expected, result)
		})
	}
}

func (s *BaseSuite) TestIsUpper() {
	tests := []struct {
		name     string
		input    string
		expected bool
	}{
		{"empty string", "", true},
		{"lowercase letters", "hello", false},
		{"uppercase letters", "HELLO", true},
		{"mixed case", "Hello", false},
		{"lowercase with numbers", "hello123", false},
		{"uppercase with numbers", "HELLO123", true},
		{"mixed case with numbers", "Hello123", false},
		{"lowercase with underscores", "hello_world", false},
		{"uppercase with underscores", "HELLO_WORLD", true},
		{"mixed case with underscores", "Hello_World", false},
		{"numbers only", "123", true},
		{"special characters only", "_@#$", true},
		{"lowercase with special chars", "hello@world", false},
		{"uppercase with special chars", "HELLO@WORLD", true},
		{"single lowercase letter", "a", false},
		{"single uppercase letter", "A", true},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			result := IsUpper(tt.input)
			s.Equal(tt.expected, result)
		})
	}
}

func TestMain(m *testing.M) {
	v := m.Run()

	snaps.Clean(m, snaps.CleanOpts{Sort: true})

	os.Exit(v)
}
