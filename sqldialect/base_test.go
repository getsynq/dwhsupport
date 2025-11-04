package sqldialect

import (
	"os"
	"testing"
	"time"

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
				OrderBy(Desc(TextCol("created_at"))).
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
				OrderBy(Desc(TextCol("created_at"))).
				WithLimit(Limit(Int64(10)))

			sql, err := sel.ToSql(dialect.Dialect)
			s.Require().NoError(err)
			snaps.WithConfig(snaps.Dir("TableFn"), snaps.Filename(dialect.Name)).MatchSnapshot(s.T(), sql)
		})
	}
}

func (s *BaseSuite) TestDialect() {
	for _, dialect := range DialectsToTest() {
		s.Run(dialect.Name, func() {
			sel := NewSelect().Cols(
				Star(),
				As(dialect.Dialect.AddTime(dialect.Dialect.CurrentTimestamp(), 24*time.Hour), Identifier("tomorrow")),
			).
				From(TableFqn("proj", "default", "users")).
				OrderBy(Desc(TextCol("created_at"))).
				WithLimit(Limit(Int64(10)))

			sql, err := sel.ToSql(dialect.Dialect)
			s.Require().NoError(err)
			snaps.WithConfig(snaps.Dir("Dialect"), snaps.Filename(dialect.Name)).MatchSnapshot(s.T(), sql)
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

func (s *BaseSuite) TestStringLiteral() {
	testCases := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "simple string",
			input:    "hello",
			expected: "'hello'",
		},
		{
			name:     "empty string",
			input:    "",
			expected: "''",
		},
		{
			name:     "string with single quote",
			input:    "it's",
			expected: "'it''s'",
		},
		{
			name:     "string with multiple single quotes",
			input:    "it's a 'test'",
			expected: "'it''s a ''test'''",
		},
		{
			name:     "string with only single quote",
			input:    "'",
			expected: "''''",
		},
		{
			name:     "string with double quotes (no escaping needed)",
			input:    `he said "hello"`,
			expected: `'he said "hello"'`,
		},
		{
			name:     "string with backslash",
			input:    `path\to\file`,
			expected: `'path\to\file'`,
		},
		{
			name:     "string with newline",
			input:    "line1\nline2",
			expected: "'line1\nline2'",
		},
	}

	for _, dialect := range DialectsToTest() {
		s.Run(dialect.Name, func() {
			for _, tc := range testCases {
				s.Run(tc.name, func() {
					result := dialect.Dialect.StringLiteral(tc.input)
					s.Equal(tc.expected, result, "Failed for input: %q", tc.input)
				})
			}
		})
	}
}

func (s *BaseSuite) TestStringExprWithEscaping() {
	for _, dialect := range DialectsToTest() {
		s.Run(dialect.Name, func() {
			sel := NewSelect().Cols(
				Star(),
				As(String("it's a test"), Identifier("test_col")),
			).
				From(TableFqn("proj", "default", "users")).
				Where(Eq(TextCol("name"), String("O'Brien")))

			sql, err := sel.ToSql(dialect.Dialect)
			s.Require().NoError(err)
			snaps.WithConfig(snaps.Dir("StringExprWithEscaping"), snaps.Filename(dialect.Name)).MatchSnapshot(s.T(), sql)
		})
	}
}

func TestMain(m *testing.M) {
	v := m.Run()

	snaps.Clean(m, snaps.CleanOpts{Sort: true})

	os.Exit(v)
}
