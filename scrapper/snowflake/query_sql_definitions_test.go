package snowflake

import (
	"testing"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/pkg/errors"
	gosnowflake "github.com/snowflakedb/gosnowflake"
	"github.com/stretchr/testify/suite"
)

type QuerySqlDefinitionsSuite struct {
	suite.Suite
}

func TestQuerySqlDefinitionsSuite(t *testing.T) {
	suite.Run(t, new(QuerySqlDefinitionsSuite))
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
		s.Equal(&scrapper.Tag{TagName: "GOVERNANCE.TAGS.TEAM", TagValue: "DATA_PLATFORM"}, tags[0])
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
