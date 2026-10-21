package snowflake

import (
	"context"
	"database/sql/driver"
	"testing"

	dwhexecsnowflake "github.com/getsynq/dwhsupport/exec/snowflake"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/pkg/errors"
	"github.com/samber/lo"
	gosnowflake "github.com/snowflakedb/gosnowflake"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type QuerySqlDefinitionsSuite struct {
	suite.Suite
}

func TestQuerySqlDefinitionsSuite(t *testing.T) {
	suite.Run(t, new(QuerySqlDefinitionsSuite))
}

func (s *QuerySqlDefinitionsSuite) TestSharedDatabases() {
	// Rows as SHOW DATABASES returns them on a real account.
	databases := []*DbDesc{
		{Name: "ANALYTICS", Kind: "STANDARD"},
		{Name: "SNOWFLAKE", Kind: "APPLICATION", Origin: "SNOWFLAKE.ACCOUNT_USAGE"},
		{Name: "PARTNER_DATA", Kind: "IMPORTED DATABASE", Origin: "ABC123.PARTNER.SHARE"},
		{Name: "MY_APP_PKG", Kind: "APPLICATION PACKAGE"},
		{Name: "REPLICA_DB", Kind: "STANDARD", Origin: "ORG.PRIMARY_ACCT.REPLICA_DB"},
		{Name: "SHARE_WITHOUT_ORIGIN", Kind: "IMPORTED DATABASE"},
	}
	s.Equal(map[string]bool{"SNOWFLAKE": true, "PARTNER_DATA": true, "SHARE_WITHOUT_ORIGIN": true}, sharedDatabases(databases))
	s.Empty(sharedDatabases(nil))
}

func (s *QuerySqlDefinitionsSuite) TestParseDdlsPerObject() {

	returnedDdl := `
create or replace schema TASKS_TESTING.PUBLIC;

create or replace TRANSIENT TABLE TASKS_TESTING.PUBLIC.MY_FIRST_DBT_MODEL (
	ID NUMBER(1,0)
);
create or replace TABLE TASKS_TESTING.PUBLIC.PARTITIONS (
	PARTITION_TIME TIMESTAMP_NTZ(9),
	DAY_OF_WEEK VARCHAR(16777216),
	IS_WEEKEND BOOLEAN,
	CREATED_AT TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP()
);
CREATE TRANSIENT TABLE mytranstable (id NUMBER, creation_date DATE);
create or replace dynamic table TASKS_TESTING.PUBLIC.PRODUCT_SUMMARY_DT(
	TYPE,
	PRODUCT_COUNT,
	TOTAL_VALUE_DOLLARS,
	AVG_PRICE_DOLLARS,
	MIN_PRICE_DOLLARS,
	MAX_PRICE_DOLLARS,
	LAST_UPDATED,
	SOURCE_TABLE
) target_lag = '1 hour' refresh_mode = AUTO initialize = ON_CREATE warehouse = COMPUTE_WH
 as
SELECT 
    type,
    COUNT(*) as product_count,
    SUM(price_dollars) as total_value_dollars,
    AVG(price_dollars) as avg_price_dollars,
    MIN(price_dollars) as min_price_dollars,
    MAX(price_dollars) as max_price_dollars,
    CURRENT_TIMESTAMP() as last_updated,
    'products' as source_table
FROM products
GROUP BY type
ORDER BY type;
create or replace view TASKS_TESTING.PUBLIC.MY_SECOND_DBT_MODEL(
	ID
) as (
    select *
  from TASKS_TESTING.PUBLIC.my_first_dbt_model
  where id = 1
  );
CREATE OR REPLACE PROCEDURE TASKS_TESTING.PUBLIC.INSERT_PARTITION_PROC()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS ' BEGIN INSERT INTO partitions (partition_time, day_of_week, is_weekend) VALUES (CURRENT_TIMESTAMP(), CASE EXTRACT(DAYOFWEEK FROM CURRENT_TIMESTAMP()) WHEN 1 THEN ''Sunday'' WHEN 2 THEN ''Monday'' WHEN 3 THEN ''Tuesday'' WHEN 4 THEN ''Wednesday'' WHEN 5 THEN ''Thursday'' WHEN 6 THEN ''Friday'' WHEN 7 THEN ''Saturday'' END, (EXTRACT(DAYOFWEEK FROM CURRENT_TIMESTAMP()) IN (1, 7))); INSERT INTO task_logs (task_name, run_time, message) VALUES (''partitions_task'', CURRENT_TIMESTAMP(), ''Partition record inserted successfully''); RETURN ''Partition record created successfully''; END; ';
CREATE OR REPLACE PROCEDURE TASKS_TESTING.PUBLIC.LOG_RESULT_PROC()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS '
DECLARE
  current_time TIMESTAMP_NTZ := CURRENT_TIMESTAMP();
  result_msg STRING;
BEGIN
  SET result_msg := ''log_result task completed - logging chain execution'';

  INSERT INTO task_logs (task_name, run_time, message) 
  VALUES (''log_result_task'', :current_time, :result_msg);

  RETURN result_msg;
END;
';
create or replace stream TASKS_TESTING.PUBLIC.RAW_PRODUCTS_STREAM on table RAW_PRODUCTS;
create or replace task TASKS_TESTING.PUBLIC.ALWAYS_SUCCESS_TASK
	warehouse=COMPUTE_WH
	schedule='120 MINUTE'
	as CALL always_success_proc();`

	perObject, err := ParseCreateStatementsPerObject(s.T().Context(), returnedDdl)
	s.Require().NoError(err)
	s.Require().Equal(6, len(perObject))
	for objectName, ddl := range perObject {
		s.NotEmpty(objectName)
		s.NotEmpty(ddl)
	}

}

func (s *QuerySqlDefinitionsSuite) TestParseDdlsPerObjectStreamlit() {
	returnedDdl := `
CREATE STREAMLIT hello_streamlit
  FROM @streamlit_db.streamlit_schema.streamlit_stage
  MAIN_FILE = 'streamlit_main.py'
  QUERY_WAREHOUSE = my_warehouse;`

	perObject, err := ParseCreateStatementsPerObject(s.T().Context(), returnedDdl)
	s.Require().NoError(err)
	s.Require().Equal(0, len(perObject))
	for objectName, ddl := range perObject {
		s.NotEmpty(objectName)
		s.NotEmpty(ddl)
	}

}

func (s *QuerySqlDefinitionsSuite) TestParseDdlsPerObjectHybridTable() {
	returnedDdl := `
CREATE OR REPLACE HYBRID TABLE ht2pk (
  col1 INTEGER NOT NULL,
  col2 INTEGER NOT NULL,
  col3 VARCHAR,
  CONSTRAINT pkey_1 PRIMARY KEY (col1, col2)
  );`

	perObject, err := ParseCreateStatementsPerObject(s.T().Context(), returnedDdl)
	s.Require().NoError(err)
	s.Require().Equal(1, len(perObject))
	for objectName, ddl := range perObject {
		s.NotEmpty(objectName)
		s.NotEmpty(ddl)
	}

}

func (s *QuerySqlDefinitionsSuite) TestParseDdlsPerObjectMaterializedView() {
	returnedDdl := `CREATE MATERIALIZED VIEW mymv
    COMMENT='Test view'
    AS
    SELECT col1, col2 FROM mytable;`

	perObject, err := ParseCreateStatementsPerObject(s.T().Context(), returnedDdl)
	s.Require().NoError(err)
	s.Require().Equal(1, len(perObject))
	for objectName, ddl := range perObject {
		s.NotEmpty(objectName)
		s.NotEmpty(ddl)
	}

}

func (s *QuerySqlDefinitionsSuite) TestParseDdlsPerObjectRecursiveView() {
	returnedDdl := `CREATE RECURSIVE VIEW employee_hierarchy_02 (title, employee_ID, manager_ID, "MGR_EMP_ID (SHOULD BE SAME)", "MGR TITLE") AS (
      -- Start at the top of the hierarchy ...
      SELECT title, employee_ID, manager_ID, NULL AS "MGR_EMP_ID (SHOULD BE SAME)", 'President' AS "MGR TITLE"
        FROM employees
        WHERE title = 'President'
      UNION ALL
      -- ... and work our way down one level at a time.
      SELECT employees.title,
             employees.employee_ID,
             employees.manager_ID,
             employee_hierarchy_02.employee_id AS "MGR_EMP_ID (SHOULD BE SAME)",
             employee_hierarchy_02.title AS "MGR TITLE"
        FROM employees INNER JOIN employee_hierarchy_02
        WHERE employee_hierarchy_02.employee_ID = employees.manager_ID
);`

	perObject, err := ParseCreateStatementsPerObject(s.T().Context(), returnedDdl)
	s.Require().NoError(err)
	s.Require().Equal(1, len(perObject))
	for objectName, ddl := range perObject {
		s.NotEmpty(objectName)
		s.NotEmpty(ddl)
	}

}

func (s *QuerySqlDefinitionsSuite) TestParseWithTagClause() {
	ddl := `create or replace TRANSIENT TABLE ANALYTICS.ANALYTICS.DATE_SPINE (
    DATE_DAY DATE
) WITH TAG (GOVERNANCE.TAGS.TEAM='DATA_INSIGHT_PLATFORM')
;`
	tags := ParseWithTagClause(ddl)
	s.Require().Len(tags, 1)
	s.Equal("GOVERNANCE.TAGS.TEAM", tags[0].TagName)
	s.Equal("DATA_INSIGHT_PLATFORM", tags[0].TagValue)
}

func (s *QuerySqlDefinitionsSuite) TestParseWithTagClauseMultipleTags() {
	ddl := `create or replace TABLE MY_DB.MY_SCHEMA.MY_TABLE (
    DS DATE,
    ID VARCHAR(16777216),
    NAME VARCHAR(256)
) WITH TAG (
    GOVERNANCE.TAGS.FRESHNESS_SLA='24',
    GOVERNANCE.TAGS.META='{"owner":"team_a","channel":"alerts"}'
)
;`
	tags := ParseWithTagClause(ddl)
	s.Require().Len(tags, 2)
	s.Equal("GOVERNANCE.TAGS.FRESHNESS_SLA", tags[0].TagName)
	s.Equal("24", tags[0].TagValue)
	s.Equal("GOVERNANCE.TAGS.META", tags[1].TagName)
	s.Equal(`{"owner":"team_a","channel":"alerts"}`, tags[1].TagValue)
}

func (s *QuerySqlDefinitionsSuite) TestParseWithTagClauseUnknownTag() {
	ddl := `create or replace TABLE MY_DB.MY_SCHEMA.MY_TABLE (
    ID VARCHAR(16777216),
    TITLE VARCHAR(16777216)
) WITH TAG (UNKNOWN_TAG='UNKNOWN_VALUE')
;`
	tags := ParseWithTagClause(ddl)
	s.Require().Empty(tags)
}

func (s *QuerySqlDefinitionsSuite) TestParseWithTagClauseMixedKnownAndUnknown() {
	ddl := `create or replace TABLE MY_DB.MY_SCHEMA.MY_TABLE (
    ID VARCHAR(16777216)
) WITH TAG (
    GOVERNANCE.TAGS.TEAM='DATA_PLATFORM',
    UNKNOWN_TAG='UNKNOWN_VALUE'
)
;`
	tags := ParseWithTagClause(ddl)
	s.Require().Len(tags, 1)
	s.Equal("GOVERNANCE.TAGS.TEAM", tags[0].TagName)
	s.Equal("DATA_PLATFORM", tags[0].TagValue)
}

func (s *QuerySqlDefinitionsSuite) TestParseWithTagClauseNoTags() {
	ddl := `create or replace TABLE MY_DB.MY_SCHEMA.MY_TABLE (
    ID VARCHAR(16777216)
)
;`
	tags := ParseWithTagClause(ddl)
	s.Require().Nil(tags)
}

func (s *QuerySqlDefinitionsSuite) TestParseWithTagClauseView() {
	ddl := `create or replace view MY_DB.MY_SCHEMA.MY_VIEW(
    ID
) WITH TAG (GOVERNANCE.TAGS.SENSITIVITY='HIGH')
 as (
    select * from MY_TABLE
);`
	tags := ParseWithTagClause(ddl)
	s.Require().Len(tags, 1)
	s.Equal("GOVERNANCE.TAGS.SENSITIVITY", tags[0].TagName)
	s.Equal("HIGH", tags[0].TagValue)
}

func (s *QuerySqlDefinitionsSuite) TestIsUnknownTag() {
	s.True(isUnknownTag("UNKNOWN_TAG", "UNKNOWN_VALUE"))
	s.True(isUnknownTag("UNKNOWN_TAG", "#UNKNOWN_VALUE"))
	s.True(isUnknownTag("DB.SCHEMA.UNKNOWN_TAG", "some_value"))
	s.False(isUnknownTag("GOVERNANCE.TAGS.TEAM", "DATA_PLATFORM"))
}

func (s *QuerySqlDefinitionsSuite) TestParseCommentClause() {
	ddl := `CREATE MATERIALIZED VIEW mymv
    COMMENT='Test view description'
    AS
    SELECT col1, col2 FROM mytable;`
	comment := ParseCommentClause(ddl)
	s.Require().NotNil(comment)
	s.Equal("Test view description", *comment)
}

func (s *QuerySqlDefinitionsSuite) TestParseCommentClauseWithoutEquals() {
	ddl := `CREATE MATERIALIZED VIEW mymv
    COMMENT 'Test view without equals'
    AS
    SELECT col1, col2 FROM mytable;`
	comment := ParseCommentClause(ddl)
	s.Require().NotNil(comment)
	s.Equal("Test view without equals", *comment)
}

func (s *QuerySqlDefinitionsSuite) TestParseCommentClauseIgnoresColumnComment() {
	ddl := `create or replace TABLE MY_DB.MY_SCHEMA.MY_TABLE (
    ID VARCHAR(16777216) COMMENT 'this is a column comment',
    NAME VARCHAR(256)
) COMMENT='this is the table comment'
;`
	comment := ParseCommentClause(ddl)
	s.Require().NotNil(comment)
	s.Equal("this is the table comment", *comment)
}

func (s *QuerySqlDefinitionsSuite) TestParseCommentClauseOnlyColumnComment() {
	ddl := `create or replace TABLE MY_DB.MY_SCHEMA.MY_TABLE (
    ID VARCHAR(16777216) COMMENT 'this is a column comment',
    NAME VARCHAR(256)
)
;`
	comment := ParseCommentClause(ddl)
	s.Nil(comment)
}

func (s *QuerySqlDefinitionsSuite) TestParseCommentClauseNoComment() {
	ddl := `create or replace TABLE MY_DB.MY_SCHEMA.MY_TABLE (
    ID VARCHAR(16777216)
);`
	comment := ParseCommentClause(ddl)
	s.Nil(comment)
}

func (s *QuerySqlDefinitionsSuite) TestParseCommentClauseEmptyComment() {
	ddl := `create or replace TABLE MY_DB.MY_SCHEMA.MY_TABLE (
    ID VARCHAR(16777216)
) COMMENT=''
;`
	comment := ParseCommentClause(ddl)
	s.Nil(comment)
}

func (s *QuerySqlDefinitionsSuite) TestParseCommentClauseWithTags() {
	ddl := `create or replace TABLE MY_DB.MY_SCHEMA.MY_TABLE (
    ID VARCHAR(16777216)
) COMMENT='My table description' WITH TAG (GOVERNANCE.TAGS.TEAM='DATA_PLATFORM')
;`
	comment := ParseCommentClause(ddl)
	s.Require().NotNil(comment)
	s.Equal("My table description", *comment)

	tags := ParseWithTagClause(ddl)
	s.Require().Len(tags, 1)
	s.Equal("GOVERNANCE.TAGS.TEAM", tags[0].TagName)
}

func (s *QuerySqlDefinitionsSuite) TestParseWithTagClausePreservesInParseDdlsPerObject() {
	ddl := `create or replace TABLE MY_DB.MY_SCHEMA.MY_TABLE (
    ID VARCHAR(16777216)
) WITH TAG (GOVERNANCE.TAGS.TEAM='DATA_PLATFORM')
;`
	perObject, err := ParseCreateStatementsPerObject(s.T().Context(), ddl)
	s.Require().NoError(err)
	s.Require().Len(perObject, 1)

	for _, objDdl := range perObject {
		tags := ParseWithTagClause(objDdl)
		s.Require().Len(tags, 1)
		s.Equal(&scrapper.Tag{TagName: "GOVERNANCE.TAGS.TEAM", TagValue: "DATA_PLATFORM", Kind: scrapper.TagKindGovernedTag}, tags[0])
	}
}

func (s *QuerySqlDefinitionsSuite) TestIsSharedDatabaseUnavailableError() {
	// Test with nil error
	s.False(isSharedDatabaseUnavailableError(nil))

	// Test with Snowflake error code 3030
	snowflakeErr := &gosnowflake.SnowflakeError{
		Number:   3030,
		SQLState: "02000",
		Message:  "Shared database is no longer available for use. It will need to be re-created if and when the publisher makes it available again.",
	}
	s.True(isSharedDatabaseUnavailableError(snowflakeErr))

	// Test with wrapped Snowflake error
	wrappedErr := errors.Wrap(snowflakeErr, "failed to query database")
	s.True(isSharedDatabaseUnavailableError(wrappedErr))

	// Test with Snowflake error with message containing the text
	snowflakeErrWithMsg := &gosnowflake.SnowflakeError{
		Number:   123,
		SQLState: "99999",
		Message:  "Some error: Shared database is no longer available for use",
	}
	s.True(isSharedDatabaseUnavailableError(snowflakeErrWithMsg))

	// Test with different Snowflake error
	differentErr := &gosnowflake.SnowflakeError{
		Number:   1234,
		SQLState: "42000",
		Message:  "Some other error",
	}
	s.False(isSharedDatabaseUnavailableError(differentErr))

	// Test with regular error
	regularErr := errors.New("regular error")
	s.False(isSharedDatabaseUnavailableError(regularErr))
}

func (s *QuerySqlDefinitionsSuite) TestGetDdlQueryQuotesEveryPart() {
	cases := []struct {
		name     string
		parts    []string
		expected string
	}{
		{
			name:     "upper-case names address the same objects quoted",
			parts:    []string{"MY_DB", "MY_SCHEMA"},
			expected: `SELECT GET_DDL('SCHEMA', '"MY_DB"."MY_SCHEMA"', TRUE)`,
		},
		{
			name:     "a lower-case schema keeps its case",
			parts:    []string{"MY_DB", "raw"},
			expected: `SELECT GET_DDL('SCHEMA', '"MY_DB"."raw"', TRUE)`,
		},
		{
			name:     "a mixed-case schema keeps its case",
			parts:    []string{"MY_DB", "Raw_staging"},
			expected: `SELECT GET_DDL('SCHEMA', '"MY_DB"."Raw_staging"', TRUE)`,
		},
		{
			name:     "a dot inside a name stays inside one identifier",
			parts:    []string{"MY_DB", "raw.v2"},
			expected: `SELECT GET_DDL('SCHEMA', '"MY_DB"."raw.v2"', TRUE)`,
		},
		{
			name:     "a double quote inside a name is doubled",
			parts:    []string{"MY_DB", `my"schema`},
			expected: `SELECT GET_DDL('SCHEMA', '"MY_DB"."my""schema"', TRUE)`,
		},
		{
			name:     "a single quote and a backslash are escaped for the string literal",
			parts:    []string{"MY_DB", `it's\raw`},
			expected: `SELECT GET_DDL('SCHEMA', '"MY_DB"."it''s\\raw"', TRUE)`,
		},
	}
	for _, c := range cases {
		s.Run(c.name, func() {
			query, err := getDdlQuery("SCHEMA", c.parts...)
			s.Require().NoError(err)
			s.Equal(c.expected, query)
		})
	}
}

// tableWithColumnTagsDdl is a table as GET_DDL writes it: a column tag sits
// inline in the column list, the table's own tags after it, and a tag created
// with a quoted lower-case name keeps its quotes.
const tableWithColumnTagsDdl = `create or replace TABLE MY_DB.MY_SCHEMA.CUSTOMERS (
	ID NUMBER(38,0) NOT NULL,
	EMAIL VARCHAR(255) NOT NULL WITH TAG (MY_DB.GOVERNANCE.PII='high'),
	SSN VARCHAR(11) WITH TAG (MY_DB.GOVERNANCE.PII='high') COMMENT 'Social security number',
	FULL_NAME VARCHAR(200),
	constraint CHK_CUSTOMERS_EMAIL check (EMAIL LIKE '%@%'),
	constraint PK_CUSTOMERS primary key (ID) rely
) WITH TAG (MY_DB.GOVERNANCE."data_domain"='customers')
COMMENT='Customer directory'
;`

// viewWithColumnTagsDdl is a view as GET_DDL writes it, with a column tag in
// its column list and two tags of its own, one holding JSON.
const viewWithColumnTagsDdl = `create or replace view MY_DB.MY_SCHEMA.ACTIVE_PRODUCTS(
	ID,
	NAME WITH TAG (MY_DB.GOVERNANCE.PII='low'),
	PRICE
) WITH TAG (MY_DB.GOVERNANCE.COST_CENTER='SALES', MY_DB.GOVERNANCE.META='{"owner":"team_a","tier":"gold"}')
 COMMENT='In-stock products'
 as
SELECT ID, NAME, PRICE
FROM MY_DB.MY_SCHEMA.PRODUCTS
WHERE IS_ACTIVE = TRUE;`

func (s *QuerySqlDefinitionsSuite) TestParseWithTagClauseTakesTheTableTagsNotAColumnTag() {
	s.Equal(
		[]*scrapper.Tag{{TagName: "MY_DB.GOVERNANCE.data_domain", TagValue: "customers", Kind: scrapper.TagKindGovernedTag}},
		ParseWithTagClause(tableWithColumnTagsDdl),
	)
}

func (s *QuerySqlDefinitionsSuite) TestParseWithTagClauseTakesTheViewTagsNotAColumnTag() {
	s.Equal(
		[]*scrapper.Tag{
			{TagName: "MY_DB.GOVERNANCE.COST_CENTER", TagValue: "SALES", Kind: scrapper.TagKindGovernedTag},
			{TagName: "MY_DB.GOVERNANCE.META", TagValue: `{"owner":"team_a","tier":"gold"}`, Kind: scrapper.TagKindGovernedTag},
		},
		ParseWithTagClause(viewWithColumnTagsDdl),
	)
}

func (s *QuerySqlDefinitionsSuite) TestParseWithTagClauseUnquotesTheTagName() {
	ddl := `create or replace TABLE MY_DB.MY_SCHEMA.MY_TABLE (
    ID NUMBER(38,0)
) WITH TAG ("My_Db".GOVERNANCE."data_domain"='customers')
;`
	s.Equal(
		[]*scrapper.Tag{{TagName: "My_Db.GOVERNANCE.data_domain", TagValue: "customers", Kind: scrapper.TagKindGovernedTag}},
		ParseWithTagClause(ddl),
	)
}

// A view's SQL comes from information_schema.views before GET_DDL is read, and
// its tags are only in GET_DDL, so they must be read from there all the same,
// without replacing the SQL.
func TestQuerySqlDefinitionsReadsTheTagsOfAViewWithSql(t *testing.T) {
	ctx := context.Background()
	viewSql := "SELECT ID, NAME, PRICE FROM MY_DB.MY_SCHEMA.PRODUCTS WHERE IS_ACTIVE = TRUE"
	fake := &fakeSnowflake{t: t, routes: []fakeRoute{
		{
			match:   sqlContains("SHOW DATABASES"),
			columns: []string{"name", "origin", "owner", "comment", "kind"},
			rows:    [][]driver.Value{{"MY_DB", "", "SYSADMIN", "", "STANDARD"}},
		},
		{
			match:   sqlContains("MY_DB.information_schema.views"),
			columns: []string{"database", "schema", "table", "is_view", "sql"},
			rows: [][]driver.Value{
				{"MY_DB", "MY_SCHEMA", "ACTIVE_PRODUCTS", true, viewSql},
				{"MY_DB", "MY_SCHEMA", "CUSTOMERS", false, ""},
			},
		},
		{
			match:   sqlContains("SHOW STREAMS IN DATABASE MY_DB"),
			columns: []string{"created_on", "name", "database_name", "schema_name", "table_name"},
		},
		{
			match:   sqlContains(`GET_DDL('SCHEMA', '"MY_DB"."MY_SCHEMA"'`),
			columns: []string{"GET_DDL"},
			rows:    [][]driver.Value{{tableWithColumnTagsDdl + "\n" + viewWithColumnTagsDdl}},
		},
	}}
	conf := &SnowflakeScrapperConf{SnowflakeConf: dwhexecsnowflake.SnowflakeConf{Account: "ORG-ACCOUNT", Databases: []string{"MY_DB"}}}
	scr := newFakeSnowflakeScrapper(ctx, fake, conf)

	rows, err := scr.QuerySqlDefinitions(ctx)
	require.NoError(t, err)
	byName := lo.KeyBy(rows, func(r *scrapper.SqlDefinitionRow) string { return r.Table })
	require.Contains(t, byName, "ACTIVE_PRODUCTS")
	require.Contains(t, byName, "CUSTOMERS")

	view := byName["ACTIVE_PRODUCTS"]
	require.Equal(t, viewSql, view.Sql, "the view keeps the SQL information_schema gave it")
	require.Equal(t, []*scrapper.Tag{
		{TagName: "MY_DB.GOVERNANCE.COST_CENTER", TagValue: "SALES", Kind: scrapper.TagKindGovernedTag},
		{TagName: "MY_DB.GOVERNANCE.META", TagValue: `{"owner":"team_a","tier":"gold"}`, Kind: scrapper.TagKindGovernedTag},
	}, view.Tags)

	require.Equal(t, map[string][]*scrapper.Tag{
		"NAME": {{TagName: "MY_DB.GOVERNANCE.PII", TagValue: "low", Kind: scrapper.TagKindGovernedTag}},
	}, view.ColumnTags)
	require.Nil(t, view.Description, "a view with SQL keeps what information_schema gave it")

	table := byName["CUSTOMERS"]
	require.Equal(t, []*scrapper.Tag{{TagName: "MY_DB.GOVERNANCE.data_domain", TagValue: "customers", Kind: scrapper.TagKindGovernedTag}}, table.Tags)
	require.Equal(t, map[string][]*scrapper.Tag{
		"EMAIL": {{TagName: "MY_DB.GOVERNANCE.PII", TagValue: "high", Kind: scrapper.TagKindGovernedTag}},
		"SSN":   {{TagName: "MY_DB.GOVERNANCE.PII", TagValue: "high", Kind: scrapper.TagKindGovernedTag}},
	}, table.ColumnTags)
	require.NotNil(t, table.Description)
	require.Equal(t, "Customer directory", *table.Description)
	require.Contains(t, table.Sql, "create or replace TABLE MY_DB.MY_SCHEMA.CUSTOMERS")
}

func (s *QuerySqlDefinitionsSuite) TestParseDdlTags() {
	cases := []struct {
		name    string
		ddl     string
		table   []*scrapper.Tag
		columns map[string][]*scrapper.Tag
	}{
		{
			name:  "table with column tags",
			ddl:   tableWithColumnTagsDdl,
			table: []*scrapper.Tag{{TagName: "MY_DB.GOVERNANCE.data_domain", TagValue: "customers", Kind: scrapper.TagKindGovernedTag}},
			columns: map[string][]*scrapper.Tag{
				"EMAIL": {{TagName: "MY_DB.GOVERNANCE.PII", TagValue: "high", Kind: scrapper.TagKindGovernedTag}},
				"SSN":   {{TagName: "MY_DB.GOVERNANCE.PII", TagValue: "high", Kind: scrapper.TagKindGovernedTag}},
			},
		},
		{
			name: "view with column tags",
			ddl:  viewWithColumnTagsDdl,
			table: []*scrapper.Tag{
				{TagName: "MY_DB.GOVERNANCE.COST_CENTER", TagValue: "SALES", Kind: scrapper.TagKindGovernedTag},
				{TagName: "MY_DB.GOVERNANCE.META", TagValue: `{"owner":"team_a","tier":"gold"}`, Kind: scrapper.TagKindGovernedTag},
			},
			columns: map[string][]*scrapper.Tag{"NAME": {{TagName: "MY_DB.GOVERNANCE.PII", TagValue: "low", Kind: scrapper.TagKindGovernedTag}}},
		},
		{
			name: "no tags at all",
			ddl:  "create or replace TABLE MY_DB.MY_SCHEMA.MY_TABLE (\n\tID NUMBER(38,0)\n) COMMENT='x';",
		},
		{
			name: "empty ddl",
			ddl:  "",
		},
		{
			name: "column tags only",
			ddl: `create or replace TABLE MY_DB.MY_SCHEMA.MY_TABLE (
	ID NUMBER(38,0),
	EMAIL VARCHAR(255) WITH TAG (MY_DB.GOVERNANCE.PII='high', MY_DB.GOVERNANCE.OWNER='crm')
);`,
			columns: map[string][]*scrapper.Tag{"EMAIL": {
				{TagName: "MY_DB.GOVERNANCE.PII", TagValue: "high", Kind: scrapper.TagKindGovernedTag},
				{TagName: "MY_DB.GOVERNANCE.OWNER", TagValue: "crm", Kind: scrapper.TagKindGovernedTag},
			}},
		},
		{
			name: "a quoted column name and a quoted tag database",
			ddl: `create or replace TABLE MY_DB.MY_SCHEMA.MY_TABLE (
	"order date" DATE WITH TAG ("my_db".GOVERNANCE.PII='low'),
	"CONSTRAINT" VARCHAR WITH TAG (MY_DB.GOVERNANCE.PII='low')
);`,
			columns: map[string][]*scrapper.Tag{
				"order date": {{TagName: "my_db.GOVERNANCE.PII", TagValue: "low", Kind: scrapper.TagKindGovernedTag}},
				"CONSTRAINT": {{TagName: "MY_DB.GOVERNANCE.PII", TagValue: "low", Kind: scrapper.TagKindGovernedTag}},
			},
		},
		{
			name: "a masked column tag is left out",
			ddl: `create or replace TABLE MY_DB.MY_SCHEMA.MY_TABLE (
	EMAIL VARCHAR(255) WITH TAG (UNKNOWN_TAG='UNKNOWN_VALUE'),
	SSN VARCHAR(11) WITH TAG (UNKNOWN_TAG='UNKNOWN_VALUE', MY_DB.GOVERNANCE.PII='high')
) WITH TAG (UNKNOWN_TAG='UNKNOWN_VALUE');`,
			columns: map[string][]*scrapper.Tag{"SSN": {{TagName: "MY_DB.GOVERNANCE.PII", TagValue: "high", Kind: scrapper.TagKindGovernedTag}}},
		},
		{
			name: "a masking policy and a default expression are not tags",
			ddl: `create or replace TABLE MY_DB.MY_SCHEMA.MY_TABLE (
	ID NUMBER(38,0) DEFAULT MY_DB.MY_SCHEMA.SEQ.NEXTVAL,
	EMAIL VARCHAR(255) WITH MASKING POLICY MY_DB.GOVERNANCE.MASK_EMAIL WITH TAG (MY_DB.GOVERNANCE.PII='high'),
	CREATED_AT TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP()
) WITH ROW ACCESS POLICY MY_DB.GOVERNANCE.RAP ON (ID) WITH TAG (MY_DB.GOVERNANCE.TEAM='core');`,
			table:   []*scrapper.Tag{{TagName: "MY_DB.GOVERNANCE.TEAM", TagValue: "core", Kind: scrapper.TagKindGovernedTag}},
			columns: map[string][]*scrapper.Tag{"EMAIL": {{TagName: "MY_DB.GOVERNANCE.PII", TagValue: "high", Kind: scrapper.TagKindGovernedTag}}},
		},
		{
			name: "a cluster key before the column list",
			ddl: `create or replace TABLE MY_DB.MY_SCHEMA.MY_TABLE cluster by (CATEGORY, ID)(
	ID NUMBER(38,0),
	NAME VARCHAR(200) WITH TAG (MY_DB.GOVERNANCE.PII='low'),
	CATEGORY VARCHAR
) WITH TAG (MY_DB.GOVERNANCE.TEAM='core');`,
			table:   []*scrapper.Tag{{TagName: "MY_DB.GOVERNANCE.TEAM", TagValue: "core", Kind: scrapper.TagKindGovernedTag}},
			columns: map[string][]*scrapper.Tag{"NAME": {{TagName: "MY_DB.GOVERNANCE.PII", TagValue: "low", Kind: scrapper.TagKindGovernedTag}}},
		},
		{
			name: "a CTE in a view's query is not read",
			ddl: `create or replace view MY_DB.MY_SCHEMA.MY_VIEW(
	ID
) as
with tag (ID) as (select 1)
select ID from tag;`,
		},
		{
			name: "a dynamic table's options before its query",
			ddl: `create or replace dynamic table MY_DB.MY_SCHEMA.MY_DT(
	ID WITH TAG (MY_DB.GOVERNANCE.PII='low')
) target_lag = '1 hour' warehouse = COMPUTE_WH WITH TAG (MY_DB.GOVERNANCE.TEAM='core')
 as
SELECT ID FROM MY_DB.MY_SCHEMA.SRC;`,
			table:   []*scrapper.Tag{{TagName: "MY_DB.GOVERNANCE.TEAM", TagValue: "core", Kind: scrapper.TagKindGovernedTag}},
			columns: map[string][]*scrapper.Tag{"ID": {{TagName: "MY_DB.GOVERNANCE.PII", TagValue: "low", Kind: scrapper.TagKindGovernedTag}}},
		},
	}
	for _, c := range cases {
		s.Run(c.name, func() {
			tags := ParseDdlTags(c.ddl)
			s.Equal(c.table, tags.Table)
			s.Equal(c.columns, tags.Columns)
		})
	}
}

// Each statement is keyed by the object's name alone, whatever follows the name
// in GET_DDL's output: a CLUSTER BY, a stream's ON TABLE, a COMMENT before AS.
// A key that ran on into those words matched no object, and the object lost its
// definition and its tags.
func (s *QuerySqlDefinitionsSuite) TestParseCreateStatementsPerObjectKeysByTheNameAlone() {
	cases := []struct {
		name string
		ddl  string
		key  string
	}{
		{
			name: "a cluster key after the name",
			ddl: `create or replace TABLE MY_DB.MY_SCHEMA.PRODUCTS cluster by (CATEGORY, ID)(
	ID NUMBER(38,0),
	CATEGORY VARCHAR(100)
);`,
			key: "MY_DB.MY_SCHEMA.PRODUCTS",
		},
		{
			name: "a cluster key on a quoted name",
			ddl: `create or replace TABLE MY_DB."my_schema"."Products" cluster by (ID)(
	ID NUMBER(38,0)
);`,
			key: `MY_DB."my_schema"."Products"`,
		},
		{
			name: "a stream on its table",
			ddl:  `create or replace stream MY_DB.MY_SCHEMA.PRODUCTS_STREAM on table PRODUCTS;`,
			key:  "MY_DB.MY_SCHEMA.PRODUCTS_STREAM",
		},
		{
			name: "a comment before a materialized view's query",
			ddl: `create or replace materialized view MY_DB.MY_SCHEMA.MY_MV COMMENT='Test view'
 as SELECT ID FROM MY_DB.MY_SCHEMA.PRODUCTS;`,
			key: "MY_DB.MY_SCHEMA.MY_MV",
		},
		{
			name: "a secure view",
			ddl: `create or replace secure view MY_DB.MY_SCHEMA.MY_VIEW(
	ID
) as SELECT ID FROM MY_DB.MY_SCHEMA.PRODUCTS;`,
			key: "MY_DB.MY_SCHEMA.MY_VIEW",
		},
		{
			name: "a dynamic table's options after the column list",
			ddl: `create or replace dynamic table MY_DB.MY_SCHEMA.MY_DT(
	ID
) target_lag = '1 hour' warehouse = COMPUTE_WH
 as SELECT ID FROM MY_DB.MY_SCHEMA.PRODUCTS;`,
			key: "MY_DB.MY_SCHEMA.MY_DT",
		},
	}
	for _, c := range cases {
		s.Run(c.name, func() {
			perObject, err := ParseCreateStatementsPerObject(s.T().Context(), c.ddl)
			s.Require().NoError(err)
			s.Equal([]string{c.key}, lo.Keys(perObject))
		})
	}
}

// A table with a cluster key gets its DDL and tags from GET_DDL like any other.
func TestQuerySqlDefinitionsReadsATableWithAClusterKey(t *testing.T) {
	ctx := context.Background()
	clusteredDdl := `create or replace TABLE MY_DB.MY_SCHEMA.PRODUCTS cluster by (CATEGORY, ID)(
	ID NUMBER(38,0) NOT NULL,
	NAME VARCHAR(200) WITH TAG (MY_DB.GOVERNANCE.PII='low'),
	CATEGORY VARCHAR(100)
) COMMENT='Product catalog' WITH TAG (MY_DB.GOVERNANCE.TEAM='core')
;`
	fake := &fakeSnowflake{t: t, routes: []fakeRoute{
		{
			match:   sqlContains("SHOW DATABASES"),
			columns: []string{"name", "origin", "owner", "comment", "kind"},
			rows:    [][]driver.Value{{"MY_DB", "", "SYSADMIN", "", "STANDARD"}},
		},
		{
			match:   sqlContains("MY_DB.information_schema.views"),
			columns: []string{"database", "schema", "table", "is_view", "sql"},
			rows:    [][]driver.Value{{"MY_DB", "MY_SCHEMA", "PRODUCTS", false, ""}},
		},
		{
			match:   sqlContains("SHOW STREAMS IN DATABASE MY_DB"),
			columns: []string{"created_on", "name", "database_name", "schema_name", "table_name"},
		},
		{
			match:   sqlContains(`GET_DDL('SCHEMA', '"MY_DB"."MY_SCHEMA"'`),
			columns: []string{"GET_DDL"},
			rows:    [][]driver.Value{{clusteredDdl}},
		},
	}}
	conf := &SnowflakeScrapperConf{SnowflakeConf: dwhexecsnowflake.SnowflakeConf{Account: "ORG-ACCOUNT", Databases: []string{"MY_DB"}}}
	scr := newFakeSnowflakeScrapper(ctx, fake, conf)

	rows, err := scr.QuerySqlDefinitions(ctx)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	table := rows[0]
	require.Contains(t, table.Sql, "create or replace TABLE MY_DB.MY_SCHEMA.PRODUCTS cluster by (CATEGORY, ID)")
	require.NotNil(t, table.Description)
	require.Equal(t, "Product catalog", *table.Description)
	require.Equal(t, []*scrapper.Tag{{TagName: "MY_DB.GOVERNANCE.TEAM", TagValue: "core"}}, table.Tags)
	require.Equal(t, map[string][]*scrapper.Tag{"NAME": {{TagName: "MY_DB.GOVERNANCE.PII", TagValue: "low"}}}, table.ColumnTags)
}
