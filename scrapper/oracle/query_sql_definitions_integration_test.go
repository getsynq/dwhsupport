package oracle

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	dwhexecoracle "github.com/getsynq/dwhsupport/exec/oracle"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scope"
	"github.com/getsynq/dwhsupport/testenv"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newOracleScrapperAsSystem connects as SYSTEM, for the tests that create a
// schema or a user of their own.
func newOracleScrapperAsSystem(t *testing.T) *OracleScrapper {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping Oracle tests in CI")
	}
	sc, err := newOracleScrapperAs(t.Context(), "system", testenv.EnvOrDefault("ORACLE_SYSTEM_PASSWORD", "SynqTest1"))
	if err != nil {
		t.Skipf("Could not connect to Oracle as SYSTEM: %v", err)
	}
	t.Cleanup(func() { _ = sc.Close() })
	return sc
}

func newOracleScrapperAs(ctx context.Context, user, password string) (*OracleScrapper, error) {
	return NewOracleScrapper(ctx, &OracleScrapperConf{OracleConf: dwhexecoracle.OracleConf{
		User:        user,
		Password:    password,
		Host:        testenv.EnvOrDefault("ORACLE_HOST", "127.0.0.1"),
		Port:        testenv.EnvOrDefaultInt("ORACLE_PORT", 1521),
		ServiceName: testenv.EnvOrDefault("ORACLE_SERVICE", "FREEPDB1"),
	}})
}

func inSchemas(schemas ...string) context.Context {
	rules := make([]scope.ScopeRule, len(schemas))
	for i, schema := range schemas {
		rules[i] = scope.ScopeRule{Schema: schema}
	}
	return scope.WithScope(scrapper.WithIncompleteResults(context.Background()), &scope.ScopeFilter{Include: rules})
}

func oracleDefinitionsByName(rows []*scrapper.SqlDefinitionRow) map[string]*scrapper.SqlDefinitionRow {
	byName := map[string]*scrapper.SqlDefinitionRow{}
	for _, row := range rows {
		byName[row.Schema+"."+row.Table] = row
	}
	return byName
}

func oracleKeys(m map[string]*scrapper.SqlDefinitionRow) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// Every table of the fixture gets its DDL, without storage or segment
// attributes, and the views keep their query. The dwhtesting synq user holds
// SELECT_CATALOG_ROLE as a grant of its own, as a customer's user would, where
// SYSTEM has it only through DBA.
func TestOracleSqlDefinitionsOfTablesAndViews(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping Oracle tests in CI")
	}
	sc, err := newOracleScrapperFromEnv(t.Context())
	if err != nil {
		t.Skipf("Could not connect to Oracle: %v", err)
	}
	t.Cleanup(func() { _ = sc.Close() })

	defs, err := sc.QuerySqlDefinitions(inSchemas("SYNQ_A", "SYNQ_B"))
	require.NoError(t, err)
	byName := oracleDefinitionsByName(defs)
	require.Len(t, byName, len(defs), "a definition is listed twice")

	for _, name := range []string{"SYNQ_A.PRODUCTS", "SYNQ_A.ORDER_ITEMS", "SYNQ_B.CUSTOMERS"} {
		def := byName[name]
		if !assert.NotNil(t, def, "%s has no definition", name) {
			continue
		}
		owner, table, _ := strings.Cut(name, ".")
		assert.True(t, def.IsTable, name)
		assert.False(t, def.IsView, name)
		assert.Equal(t, sc.conf.ServiceName, def.Database)
		assert.True(t, strings.HasPrefix(def.Sql, `CREATE TABLE "`+owner+`"."`+table+`"`), "%s: %s", name, def.Sql)
		for _, attribute := range []string{"TABLESPACE", "STORAGE", "PCTFREE", "INITRANS", "LOGGING"} {
			assert.NotContains(t, def.Sql, attribute, name)
		}
	}
	assert.Contains(t, lo.FromPtr(byName["SYNQ_A.ORDER_ITEMS"]).Sql, `REFERENCES "SYNQ_A"."PRODUCTS" ("ID")`)

	for _, name := range []string{"SYNQ_A.ACTIVE_PRODUCTS", "SYNQ_A.ORDER_SUMMARY", "SYNQ_B.CUSTOMER_REGIONS"} {
		def := byName[name]
		if !assert.NotNil(t, def, "%s has no definition", name) {
			continue
		}
		assert.True(t, def.IsView, name)
		assert.False(t, def.IsTable, name)
		assert.NotContains(t, def.Sql, "CREATE TABLE", name)
	}
}

// The objects ALL_TABLES lists beside a table, a materialized view's container,
// an index-organized table's overflow segment and a nested table's storage,
// have no DDL of their own. They are made in a schema of the test's own, as
// the fixture has none.
func TestOracleSqlDefinitionsLeaveOutTablesThatAreNotTablesOfTheirOwn(t *testing.T) {
	sc := newOracleScrapperAsSystem(t)
	ctx := t.Context()
	schema := fmt.Sprintf("SYNQ_DDL_%d", time.Now().UnixNano())
	exec := func(statement string) {
		require.NoError(t, sc.executor.Exec(ctx, statement), statement)
	}
	exec(`CREATE USER ` + schema + ` IDENTIFIED BY "SynqTest1" QUOTA UNLIMITED ON USERS`)
	t.Cleanup(func() { _ = sc.executor.Exec(context.Background(), `DROP USER `+schema+` CASCADE`) })
	exec(`GRANT CREATE TABLE, CREATE MATERIALIZED VIEW TO ` + schema)
	exec(`CREATE TABLE ` + schema + `.plain (id NUMBER PRIMARY KEY, name VARCHAR2(100))`)
	exec(`CREATE TABLE ` + schema + `.organized (id NUMBER PRIMARY KEY, note VARCHAR2(4000)) ` +
		`ORGANIZATION INDEX INCLUDING id OVERFLOW`)
	exec(`CREATE TYPE ` + schema + `.tags_t AS TABLE OF VARCHAR2(30)`)
	exec(`CREATE TABLE ` + schema + `.tagged (id NUMBER, tags ` + schema + `.tags_t) NESTED TABLE tags STORE AS tagged_tags`)
	exec(`CREATE MATERIALIZED VIEW ` + schema + `.plain_names AS SELECT id, name FROM ` + schema + `.plain`)

	defs, err := sc.QuerySqlDefinitions(inSchemas(schema))
	require.NoError(t, err)
	byName := oracleDefinitionsByName(defs)
	assert.ElementsMatch(t, []string{schema + ".PLAIN", schema + ".ORGANIZED", schema + ".TAGGED"}, oracleKeys(byName))
	assert.Contains(t, lo.FromPtr(byName[schema+".ORGANIZED"]).Sql, "ORGANIZATION INDEX")
	assert.Contains(t, lo.FromPtr(byName[schema+".TAGGED"]).Sql, "NESTED TABLE")
}

// A user with SELECT ANY DICTIONARY and no SELECT_CATALOG_ROLE reads the views,
// and each schema of tables is one omission saying which grant reads it. The
// user is the test's own, as the dwhtesting synq user has the role.
func TestOracleSqlDefinitionsWithoutTheCatalogRoleOmitTheSchemas(t *testing.T) {
	system := newOracleScrapperAsSystem(t)
	ctx := t.Context()
	user := fmt.Sprintf("SYNQ_NO_CATALOG_%d", time.Now().UnixNano())
	exec := func(statement string) {
		require.NoError(t, system.executor.Exec(ctx, statement), statement)
	}
	exec(`CREATE USER ` + user + ` IDENTIFIED BY "SynqTest1"`)
	t.Cleanup(func() { _ = system.executor.Exec(context.Background(), `DROP USER `+user+` CASCADE`) })
	exec(`GRANT CREATE SESSION, SELECT ANY TABLE, SELECT ANY DICTIONARY TO ` + user)

	sc, err := newOracleScrapperAs(ctx, user, "SynqTest1")
	require.NoError(t, err)
	// Cleanups run last first, so the session is gone before the user is dropped.
	t.Cleanup(func() { _ = sc.Close() })

	defs, err := sc.QuerySqlDefinitions(inSchemas("SYNQ_A"))
	incomplete, ok := scrapper.AsIncomplete(err)
	require.True(t, ok, "%v", err)
	assert.Equal(t, []scrapper.Omission{
		{Database: sc.conf.ServiceName, Schema: "SYNQ_A", Part: scrapper.OmittedDefinitions, Err: errNoCatalogRole},
	}, incomplete.Omissions)
	assert.ElementsMatch(t, []string{"SYNQ_A.ACTIVE_PRODUCTS", "SYNQ_A.ORDER_SUMMARY"}, oracleKeys(oracleDefinitionsByName(defs)))
}
