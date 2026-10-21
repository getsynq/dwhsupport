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

// newOracleScrapperAsSystem connects as SYSTEM, whose DBA role includes
// SELECT_CATALOG_ROLE. The dwhtesting synq user has SELECT ANY DICTIONARY
// only, which DBMS_METADATA does not take for another schema's objects.
func newOracleScrapperAsSystem(t *testing.T) *OracleScrapper {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping Oracle tests in CI")
	}
	sc, err := NewOracleScrapper(t.Context(), &OracleScrapperConf{OracleConf: dwhexecoracle.OracleConf{
		User:        "system",
		Password:    testenv.EnvOrDefault("ORACLE_SYSTEM_PASSWORD", "SynqTest1"),
		Host:        testenv.EnvOrDefault("ORACLE_HOST", "127.0.0.1"),
		Port:        testenv.EnvOrDefaultInt("ORACLE_PORT", 1521),
		ServiceName: testenv.EnvOrDefault("ORACLE_SERVICE", "FREEPDB1"),
	}})
	if err != nil {
		t.Skipf("Could not connect to Oracle as SYSTEM: %v", err)
	}
	t.Cleanup(func() { _ = sc.Close() })
	return sc
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
// attributes, and the views keep their query.
func TestOracleSqlDefinitionsOfTablesAndViews(t *testing.T) {
	sc := newOracleScrapperAsSystem(t)

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

// The dwhtesting synq user has no SELECT_CATALOG_ROLE: it reads the views, and
// each schema of tables is one omission saying which grant reads it.
func TestOracleSqlDefinitionsWithoutTheCatalogRoleOmitTheSchemas(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping Oracle tests in CI")
	}
	sc, err := newOracleScrapperFromEnv(t.Context())
	if err != nil {
		t.Skipf("Could not connect to Oracle: %v", err)
	}
	t.Cleanup(func() { _ = sc.Close() })

	defs, err := sc.QuerySqlDefinitions(inSchemas("SYNQ_A"))
	incomplete, ok := scrapper.AsIncomplete(err)
	require.True(t, ok, "%v", err)
	assert.Equal(t, []scrapper.Omission{
		{Database: sc.conf.ServiceName, Schema: "SYNQ_A", Part: scrapper.OmittedDefinitions, Err: errNoCatalogRole},
	}, incomplete.Omissions)
	assert.ElementsMatch(t, []string{"SYNQ_A.ACTIVE_PRODUCTS", "SYNQ_A.ORDER_SUMMARY"}, oracleKeys(oracleDefinitionsByName(defs)))
}
