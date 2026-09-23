package sqldialect

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func mustSql(t *testing.T, e Expr, d Dialect) string {
	t.Helper()
	sql, err := e.ToSql(d)
	require.NoError(t, err)
	return sql
}

// The whole point of the type: a reserved word carries no character that the
// "quote when needed" helpers react to, so they emit it bare.
func TestIdentQuotesAReservedWord(t *testing.T) {
	cases := map[string]struct {
		dialect Dialect
		want    string
	}{
		"postgres":   {NewPostgresDialect(), `"order"`},
		"snowflake":  {NewSnowflakeDialect(), `"ORDER"`},
		"oracle":     {NewOracleDialect(), `"ORDER"`},
		"duckdb":     {NewDuckDBDialect(), `"order"`},
		"trino":      {NewTrinoDialect(), `"order"`},
		"redshift":   {NewRedshiftDialect(), `"order"`},
		"bigquery":   {NewBigQueryDialect(), "`order`"},
		"clickhouse": {NewClickHouseDialect(), "`order`"},
		"mysql":      {NewMySQLDialect(), "`order`"},
		"databricks": {NewDatabricksDialect(), "`order`"},
		"mssql":      {NewMSSQLDialect(), "[order]"},
		"fabric":     {NewFabricDialect(), "[order]"},
		"db2":        {NewDb2Dialect(), `"ORDER"`},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, c.want, mustSql(t, WrittenIdent("order"), c.dialect))
		})
	}
}

// Db2 folds an unquoted name to upper case and takes #, $ and @ inside one, but
// rejects one that starts with an underscore, which therefore has to be quoted
// as written.
func TestDb2Identifiers(t *testing.T) {
	d := NewDb2Dialect()
	assert.Equal(t, `"SALES#Q1"`, mustSql(t, WrittenIdent("sales#q1"), d))
	assert.Equal(t, `"A@B"`, mustSql(t, WrittenIdent("a@b"), d))
	assert.Equal(t, `"_recon_base"`, d.ResolveFieldRef("_recon_base"))
	assert.Equal(t, "created_at", d.ResolveFieldRef("created_at"))
	assert.Equal(t, `"createdAt"`, d.ResolveFieldRef("createdAt"))
}

// Quoting pins the case, so a written name is folded first: the quoted form has
// to address the object the unquoted form addressed.
func TestWrittenIdentFoldsAnUnquotedName(t *testing.T) {
	assert.Equal(t, `"USERS"`, mustSql(t, WrittenIdent("users"), NewSnowflakeDialect()))
	assert.Equal(t, `"MYTABLE"`, mustSql(t, WrittenIdent("MyTable"), NewSnowflakeDialect()))
	assert.Equal(t, `"users"`, mustSql(t, WrittenIdent("USERS"), NewPostgresDialect()))
	assert.Equal(t, `"mytable"`, mustSql(t, WrittenIdent("MyTable"), NewPostgresDialect()))
	assert.Equal(t, "`MyTable`", mustSql(t, WrittenIdent("MyTable"), NewBigQueryDialect()))
}

// Quotes the author wrote are the author saying "this is the exact name".
func TestWrittenIdentHonoursQuotesTheAuthorWrote(t *testing.T) {
	assert.Equal(t, `"events"`, mustSql(t, WrittenIdent(`"events"`), NewSnowflakeDialect()))
	assert.Equal(t, `"MyTable"`, mustSql(t, WrittenIdent(`"MyTable"`), NewPostgresDialect()))
	assert.Equal(t, "`MyTable`", mustSql(t, WrittenIdent("`MyTable`"), NewBigQueryDialect()))
	assert.Equal(t, "[MyTable]", mustSql(t, WrittenIdent("[MyTable]"), NewMSSQLDialect()))
}

// A name the engine handed back is already canonical, so it is rendered as-is
// whatever the dialect would have folded it to.
func TestCanonicalIdentDoesNotFold(t *testing.T) {
	assert.Equal(t, `"MyCol"`, mustSql(t, CanonicalIdent("MyCol"), NewSnowflakeDialect()))
	assert.Equal(t, `"MyCol"`, mustSql(t, CanonicalIdent("MyCol"), NewPostgresDialect()))
	// A name the engine handed back is literal all the way down: a column
	// really called `"MyCol"`, quotes included, keeps them.
	assert.Equal(t, `"""MyCol"""`, mustSql(t, CanonicalIdent(`"MyCol"`), NewPostgresDialect()))
}

func TestIdentEscapesTheClosingDelimiter(t *testing.T) {
	assert.Equal(t, `"we""ird"`, mustSql(t, CanonicalIdent(`we"ird`), NewPostgresDialect()))
	// BigQuery is the exception: "Quoted identifiers have the same escape
	// sequences as string literals", so its delimiter is backslashed.
	assert.Equal(t, "`we\\`ird`", mustSql(t, CanonicalIdent("we`ird"), NewBigQueryDialect()))
	assert.Equal(t, "`we``ird`", mustSql(t, CanonicalIdent("we`ird"), NewMySQLDialect()))
	assert.Equal(t, "[we]]ird]", mustSql(t, CanonicalIdent("we]ird"), NewMSSQLDialect()))
}

// Unwrapping undoes the doubling, so text a person wrote round trips to
// itself rather than growing a delimiter every pass.
func TestIdentRoundTripsAnEscapedDelimiter(t *testing.T) {
	assert.Equal(t, `"we""ird"`, mustSql(t, WrittenIdent(`"we""ird"`), NewPostgresDialect()))
}

func TestQualifiedIdentQuotesEachPartOnItsOwn(t *testing.T) {
	fqn := QualifiedIdent(
		WrittenIdent("PROD_DATABASE"),
		WrittenIdent("ECO2_ORDERS_AIRBYTE"),
		WrittenIdent("ORDER"),
	)
	assert.Equal(t,
		`"PROD_DATABASE"."ECO2_ORDERS_AIRBYTE"."ORDER"`,
		mustSql(t, fqn, NewSnowflakeDialect()),
	)
}

func TestQualifiedIdentSkipsAnAbsentPart(t *testing.T) {
	fqn := QualifiedIdent(WrittenIdent(""), WrittenIdent("public"), WrittenIdent("orders"))
	assert.Equal(t, `"public"."orders"`, mustSql(t, fqn, NewPostgresDialect()))
}

// `"public.table"` is one object and `"public"."table"` is another, so the
// split has to respect the quotes rather than count dots.
func TestSplitQualifiedIdent(t *testing.T) {
	cases := []struct {
		text string
		want []string
	}{
		{"db.schema.table", []string{"db", "schema", "table"}},
		{"table", []string{"table"}},
		{`"public.table"`, []string{`"public.table"`}},
		{`"a.b".c`, []string{`"a.b"`, "c"}},
		{"`a.b`.c", []string{"`a.b`", "c"}},
		{"[a.b].c", []string{"[a.b]", "c"}},
		{`db."my.schema".t`, []string{"db", `"my.schema"`, "t"}},
		{`"unterminated.x`, []string{`"unterminated.x`}},
		// A doubled closing delimiter is one literal delimiter, not the end of
		// the quote. MSSQL's `]]` is the case that shows it: `"` and backticks
		// survive a naive scan only because the second one reopens the quote.
		{`[a]]b.c]`, []string{`[a]]b.c]`}},
		{`[a]]b].c`, []string{`[a]]b]`, "c"}},
		{`"a""b.c"`, []string{`"a""b.c"`}},
		{"`a``b.c`", []string{"`a``b.c`"}},
	}
	for _, c := range cases {
		t.Run(c.text, func(t *testing.T) {
			assert.Equal(t, c.want, SplitQualifiedIdent(c.text))
		})
	}
}

func TestSplitQualifiedIdentFeedsIdentsThatSurviveTheDot(t *testing.T) {
	parts := SplitQualifiedIdent(`public."my.table"`)
	idents := make([]Ident, len(parts))
	for i, p := range parts {
		idents[i] = WrittenIdent(p)
	}
	assert.Equal(t, `"public"."my.table"`, mustSql(t, QualifiedIdent(idents...), NewPostgresDialect()))
}

// The split and the unwrap have to agree about an escaped delimiter, or a name
// containing one comes back as two parts or loses a character on the way.
func TestSplitQualifiedIdentRoundTripsAnEscapedDelimiter(t *testing.T) {
	cases := map[string]struct {
		text    string
		name    string
		dialect Dialect
	}{
		"mssql":    {`[a]]b.c]`, "a]b.c", NewMSSQLDialect()},
		"postgres": {`"a""b.c"`, `a"b.c`, NewPostgresDialect()},
		"bigquery": {"`a\\`b.c`", "a`b.c", NewBigQueryDialect()},
		"mysql":    {"`a``b.c`", "a`b.c", NewMySQLDialect()},
	}
	for label, c := range cases {
		t.Run(label, func(t *testing.T) {
			parts := SplitQualifiedIdent(c.text)
			require.Len(t, parts, 1)

			ident := WrittenIdent(parts[0])
			assert.Equal(t, c.name, ident.Name(c.dialect))
			assert.Equal(t, c.text, mustSql(t, ident, c.dialect))
		})
	}
}

// BigQuery quoted identifiers take the string-literal escapes, so `\x41` is
// the identifier `A`. Dropping only the backslash would leave `x41`, which is
// a different object rather than a syntax error.
func TestWrittenIdentDecodesBigQueryEscapes(t *testing.T) {
	bq := NewBigQueryDialect()
	cases := map[string]string{
		"`\\x41`":       "A",
		"`\\u0041`":     "A",
		"`\\U00000041`": "A",
		"`\\101`":       "A",
		"`a\\tb`":       "a\tb",
		"`we\\`ird`":    "we`ird",
		"`a\\\\b`":      `a\b`,
	}
	for text, want := range cases {
		t.Run(text, func(t *testing.T) {
			assert.Equal(t, want, WrittenIdent(text).Name(bq))
		})
	}
}

// Oracle takes `#` in an unquoted identifier and folds it up, so quoting the
// name without folding would address a different object.
func TestWrittenIdentFoldsNamesEachEngineWouldHaveTakenUnquoted(t *testing.T) {
	assert.Equal(t, `"SALES#Q1"`, mustSql(t, WrittenIdent("sales#q1"), NewOracleDialect()))
	assert.Equal(t, `"SALES$Q1"`, mustSql(t, WrittenIdent("sales$q1"), NewSnowflakeDialect()))
	// Postgres folds letters with diacritics the same as any other letter.
	assert.Equal(t, `"école"`, mustSql(t, WrittenIdent("ÉCOLE"), NewPostgresDialect()))
	// Snowflake's unquoted grammar is ASCII, so a non-ASCII name could only
	// ever have been quoted — it is left as written rather than upper-cased.
	assert.Equal(t, `"zamówienia"`, mustSql(t, WrittenIdent("zamówienia"), NewSnowflakeDialect()))
	// A name no engine would take unquoted keeps its case on every dialect.
	assert.Equal(t, `"Created At"`, mustSql(t, WrittenIdent("Created At"), NewSnowflakeDialect()))
	assert.Equal(t, `"Created At"`, mustSql(t, WrittenIdent("Created At"), NewPostgresDialect()))
}

// A backslash is a literal character inside a double-quoted or bracketed
// identifier, so it must not swallow the closing delimiter there.
func TestSplitQualifiedIdentTreatsBackslashAsAnEscapeOnlyInBackticks(t *testing.T) {
	assert.Equal(t, []string{`"a\\"`, "orders"}, SplitQualifiedIdent(`"a\\".orders`))
	assert.Equal(t, []string{`[a\\]`, "orders"}, SplitQualifiedIdent(`[a\\].orders`))
	assert.Equal(t, []string{"`a\\`b.c`"}, SplitQualifiedIdent("`a\\`b.c`"))
}
