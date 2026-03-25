package dbtprofiles

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadConnections(t *testing.T) {
	t.Setenv("DBT_SNOWFLAKE_PASSWORD", "snow_secret")
	t.Setenv("DBT_SNOWFLAKE_PROD_PASSWORD", "prod_secret")
	t.Setenv("DBT_REDSHIFT_PASSWORD", "rs_secret")
	t.Setenv("DBT_DATABRICKS_TOKEN", "dapi_token")

	conns, err := LoadConnections("testdata/profiles.yml")
	require.NoError(t, err)

	// analytics/dev, analytics/prod, analytics (default=dev) = 3
	// warehouse/local, warehouse/staging, warehouse (default=local) = 3
	// bigquery_project/dev, bigquery_project (default=dev) = 2
	// databricks_project/dev, databricks_project (default=dev) = 2
	// Total = 10
	assert.Len(t, conns, 10)

	// Verify Snowflake (default target)
	sf := conns["analytics"]
	require.NotNil(t, sf)
	assert.Equal(t, "snowflake", sf.DialectType())
	require.NotNil(t, sf.Snowflake)
	assert.Equal(t, "xy12345.eu-west-1", sf.Snowflake.Account)
	assert.Equal(t, "dbt_user", sf.Snowflake.Username)
	assert.Equal(t, "snow_secret", sf.Snowflake.Password)
	assert.Equal(t, []string{"ANALYTICS"}, sf.Snowflake.Databases)
	assert.Equal(t, "TRANSFORMING", sf.Snowflake.Warehouse)
	assert.Equal(t, "DBT_ROLE", sf.Snowflake.Role)

	// Same as analytics/dev
	sfDev := conns["analytics/dev"]
	require.NotNil(t, sfDev)
	assert.Equal(t, sf, sfDev)

	// Snowflake prod target
	sfProd := conns["analytics/prod"]
	require.NotNil(t, sfProd)
	require.NotNil(t, sfProd.Snowflake)
	assert.Equal(t, "prod_secret", sfProd.Snowflake.Password)
	assert.Equal(t, "DBT_PROD_ROLE", sfProd.Snowflake.Role)

	// Postgres
	pg := conns["warehouse"]
	require.NotNil(t, pg)
	assert.Equal(t, "postgres", pg.DialectType())
	require.NotNil(t, pg.Postgres)
	assert.Equal(t, "localhost", pg.Postgres.Host)
	assert.Equal(t, 5432, pg.Postgres.Port)
	assert.Equal(t, "warehouse", pg.Postgres.Database)

	// Redshift
	rs := conns["warehouse/staging"]
	require.NotNil(t, rs)
	assert.Equal(t, "redshift", rs.DialectType())
	require.NotNil(t, rs.Redshift)
	assert.Equal(t, "rs_secret", rs.Redshift.Password)
	assert.Equal(t, 5439, rs.Redshift.Port)

	// BigQuery
	bq := conns["bigquery_project"]
	require.NotNil(t, bq)
	assert.Equal(t, "bigquery", bq.DialectType())
	require.NotNil(t, bq.BigQuery)
	assert.Equal(t, "my-gcp-project", bq.BigQuery.ProjectId)
	assert.Equal(t, "US", bq.BigQuery.Region)
	assert.Equal(t, "/path/to/keyfile.json", bq.BigQuery.ServiceAccountKeyFile)

	// Databricks
	dbr := conns["databricks_project"]
	require.NotNil(t, dbr)
	assert.Equal(t, "databricks", dbr.DialectType())
	require.NotNil(t, dbr.Databricks)
	assert.Equal(t, "https://dbc-12345.cloud.databricks.com", dbr.Databricks.WorkspaceUrl)
	assert.Equal(t, "dapi_token", dbr.Databricks.AuthToken)
	assert.Equal(t, "/sql/1.0/warehouses/abc123", dbr.Databricks.Warehouse)
}

func TestLoadConnections_EnvVarDefault(t *testing.T) {
	// DBT_SNOWFLAKE_PASSWORD not set — should use default value
	t.Setenv("DBT_SNOWFLAKE_PROD_PASSWORD", "prod_pw")
	t.Setenv("DBT_REDSHIFT_PASSWORD", "rs_pw")
	t.Setenv("DBT_DATABRICKS_TOKEN", "tok")

	conns, err := LoadConnections("testdata/profiles.yml")
	require.NoError(t, err)

	sf := conns["analytics/dev"]
	require.NotNil(t, sf)
	require.NotNil(t, sf.Snowflake)
	assert.Equal(t, "default_pass", sf.Snowflake.Password)
}

func TestLoadConnections_FileNotFound(t *testing.T) {
	_, err := LoadConnections("testdata/nonexistent.yml")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "dbtprofiles")
}

func TestLoadProtoConnections(t *testing.T) {
	t.Setenv("DBT_SNOWFLAKE_PASSWORD", "pw")
	t.Setenv("DBT_SNOWFLAKE_PROD_PASSWORD", "pw")
	t.Setenv("DBT_REDSHIFT_PASSWORD", "pw")
	t.Setenv("DBT_DATABRICKS_TOKEN", "tok")

	protos, err := LoadProtoConnections("testdata/profiles.yml")
	require.NoError(t, err)
	assert.NotEmpty(t, protos)

	// Verify proto conversion worked
	sf := protos["analytics"]
	require.NotNil(t, sf)
	sfConf := sf.GetSnowflake()
	require.NotNil(t, sfConf)
	assert.Equal(t, "xy12345.eu-west-1", sfConf.GetAccount())
}

func TestResolveEnvVars(t *testing.T) {
	t.Setenv("TEST_VAR", "hello")

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "simple env var",
			input:    `{{ env_var('TEST_VAR') }}`,
			expected: "hello",
		},
		{
			name:     "with default, var set",
			input:    `{{ env_var('TEST_VAR', 'fallback') }}`,
			expected: "hello",
		},
		{
			name:     "with default, var unset",
			input:    `{{ env_var('UNSET_VAR', 'fallback') }}`,
			expected: "fallback",
		},
		{
			name:     "unset without default",
			input:    `{{ env_var('UNSET_VAR') }}`,
			expected: `{{ env_var('UNSET_VAR') }}`,
		},
		{
			name:     "double quotes",
			input:    `{{ env_var("TEST_VAR") }}`,
			expected: "hello",
		},
		{
			name:     "with spaces",
			input:    `{{  env_var( 'TEST_VAR' )  }}`,
			expected: "hello",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, resolveEnvVars(tt.input))
		})
	}
}
