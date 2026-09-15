package sqldialect

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Oracle is the one dialect that rejects the optional AS before a derived
// table's alias, with ORA-03048. Every other dialect documents it as accepted,
// so the keyword is a per-dialect answer rather than something dropped
// everywhere.
func TestSubqueryTableAliasKeyword(t *testing.T) {
	for _, dialect := range DialectsToTest() {
		sql, err := SubqueryTable("select 1", "src").ToSql(dialect.Dialect)
		require.NoError(t, err)

		if dialect.Name == "oracle" {
			require.NotContains(t, sql, ") AS ", "%s must not put AS before a table alias", dialect.Name)
			continue
		}
		require.Contains(t, sql, ") AS ", "%s should keep AS before a table alias", dialect.Name)
	}
}

// The alias a derived table is given and a reference to that alias have to
// resolve identically, or the reference names something else once the dialect
// folds case.
func TestSubqueryTableAliasMatchesQualifiedCol(t *testing.T) {
	for _, dialect := range DialectsToTest() {
		sql, err := SubqueryTable("select 1", "_recon_base").ToSql(dialect.Dialect)
		require.NoError(t, err)

		emitted := strings.TrimSpace(strings.TrimPrefix(sql[strings.LastIndex(sql, ")")+1:], " AS"))

		ref, err := QualifiedCol("_recon_base", "id").ToSql(dialect.Dialect)
		require.NoError(t, err)
		qualifier := ref[:strings.LastIndex(ref, ".")]

		require.Equal(t, emitted, qualifier, "%s: alias %q and qualifier %q must agree", dialect.Name, emitted, qualifier)
	}
}

// Oracle requires an unquoted identifier to begin with a letter, so a generated
// alias like `_recon_base` has to be quoted there and nowhere else.
func TestOracleQuotesIdentifiersNotStartingWithALetter(t *testing.T) {
	d := NewOracleDialect()

	require.Equal(t, `"_recon_base"`, d.ResolveFieldRef("_recon_base"))
	require.Equal(t, `"1st_col"`, d.ResolveFieldRef("1st_col"))
	require.Equal(t, "ID", d.ResolveFieldRef("ID"))
	require.Equal(t, "id", d.ResolveFieldRef("id"))
	require.Equal(t, `"mixedCase"`, d.ResolveFieldRef("mixedCase"))

	// Already-quoted input is passed through, not quoted twice.
	require.Equal(t, `"_recon_base"`, d.ResolveFieldRef(`"_recon_base"`))
}
