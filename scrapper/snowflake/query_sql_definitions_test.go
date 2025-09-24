package snowflake

import (
	"testing"

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
