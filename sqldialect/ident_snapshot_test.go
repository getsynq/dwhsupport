package sqldialect

import (
	"fmt"
	"strings"
	"testing"

	"github.com/gkampitakis/go-snaps/snaps"
	"github.com/stretchr/testify/suite"
)

type IdentSuite struct {
	suite.Suite
}

func TestIdentSuite(t *testing.T) {
	suite.Run(t, new(IdentSuite))
}

// identCases is the set every dialect is rendered against, so one golden file
// per dialect shows that dialect's whole answer side by side. Each label says
// what a customer would have written, because that is what decides which
// constructor is right.
var identCases = []struct {
	label string
	text  string
}{
	{"reserved word", "order"},
	{"reserved word, upper", "ORDER"},
	{"plain lower", "orders"},
	{"plain upper", "ORDERS"},
	{"mixed case", "MyTable"},
	{"leading digit", "1st_table"},
	{"space", "Created At"},
	{"dash", "my-table"},
	{"dot", "my.table"},
	{"non-ascii", "zamówienia"},
	{"double quote inside", `we"ird`},
	{"backtick inside", "we`ird"},
	{"bracket inside", "we]ird"},
	{"backslash inside", `we\ird`},
	{"pre-quoted, double quotes", `"events"`},
	{"pre-quoted, backticks", "`events`"},
	{"pre-quoted, brackets", "[events]"},
	{"pre-quoted with an escaped delimiter", `"we""ird"`},
	{"empty", ""},
}

// TestWrittenIdent is the form a config file carries: what the author typed,
// which may or may not already be quoted.
func (s *IdentSuite) TestWrittenIdent() {
	for _, dialect := range DialectsToTest() {
		s.Run(dialect.Name, func() {
			snaps.WithConfig(snaps.Dir("WrittenIdent"), snaps.Filename(dialect.Name)).
				MatchSnapshot(s.T(), renderIdentCases(s.T(), dialect.Dialect, WrittenIdent))
		})
	}
}

// TestCanonicalIdent is the form the engine itself handed back — a QueryShape
// column, a catalog row — where the case is already the object's own.
func (s *IdentSuite) TestCanonicalIdent() {
	for _, dialect := range DialectsToTest() {
		s.Run(dialect.Name, func() {
			snaps.WithConfig(snaps.Dir("CanonicalIdent"), snaps.Filename(dialect.Name)).
				MatchSnapshot(s.T(), renderIdentCases(s.T(), dialect.Dialect, CanonicalIdent))
		})
	}
}

// TestQualifiedIdent covers the FROM clause a table reference becomes,
// including the parts a customer is most likely to get wrong: a reserved word
// as the table, a part they quoted themselves, and an absent database.
func (s *IdentSuite) TestQualifiedIdent() {
	cases := []struct {
		label string
		parts []string
	}{
		{"three parts", []string{"PROD_DATABASE", "ECO2_ORDERS_AIRBYTE", "ORDER"}},
		{"two parts", []string{"public", "order"}},
		{"one part", []string{"orders"}},
		{"database absent", []string{"", "public", "orders"}},
		{"schema absent", []string{"proj", "", "orders"}},
		{"author quoted the table", []string{"DB", "SCH", `"events"`}},
		{"author quoted with backticks", []string{"proj", "ds", "`events`"}},
		{"mixed case throughout", []string{"MyProject", "MySchema", "MyTable"}},
		{"dash in the project", []string{"my-project", "dataset", "table"}},
	}
	for _, dialect := range DialectsToTest() {
		s.Run(dialect.Name, func() {
			var out strings.Builder
			for _, c := range cases {
				idents := make([]Ident, len(c.parts))
				for i, p := range c.parts {
					idents[i] = WrittenIdent(p)
				}
				sql, err := QualifiedIdent(idents...).ToSql(dialect.Dialect)
				s.Require().NoError(err)
				fmt.Fprintf(&out, "%-30s %s\n", c.label, sql)
			}
			snaps.WithConfig(snaps.Dir("QualifiedIdent"), snaps.Filename(dialect.Name)).
				MatchSnapshot(s.T(), out.String())
		})
	}
}

// TestIdentRoundTrip is the invariant the two halves have to keep: rendering a
// name and reading it back returns the name, whatever the delimiter and escape.
// A dialect whose quoting and unquoting disagree loses or grows a character
// every pass.
func (s *IdentSuite) TestIdentRoundTrip() {
	names := []string{
		"order", "orders", "MyTable", "Created At", "my.table", "my-table",
		"zamówienia", "we\"ird", "we`ird", "we]ird", "we\\ird", "a\"b.`c]",
	}
	for _, dialect := range DialectsToTest() {
		s.Run(dialect.Name, func() {
			for _, name := range names {
				quoted := dialect.Dialect.QuoteIdent(name)
				back, wasQuoted := dialect.Dialect.UnquoteIdent(quoted)
				s.True(wasQuoted, "%q rendered as %s should read back as quoted", name, quoted)
				s.Equal(name, back, "%q rendered as %s did not round trip", name, quoted)
			}
		})
	}
}

func renderIdentCases(t *testing.T, dialect Dialect, build func(string) Ident) string {
	t.Helper()
	var out strings.Builder
	for _, c := range identCases {
		ident := build(c.text)
		sql, err := ident.ToSql(dialect)
		if err != nil {
			t.Fatalf("%s: %v", c.label, err)
		}
		fmt.Fprintf(&out, "%-38s %-16q %-24s resolves to %q\n", c.label, c.text, sql, ident.Name(dialect))
	}
	return out.String()
}
