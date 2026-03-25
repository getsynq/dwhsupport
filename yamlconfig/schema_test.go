package yamlconfig

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConnectionsSchema(t *testing.T) {
	schema := ConnectionsSchema()
	require.NotNil(t, schema)

	data, err := json.MarshalIndent(schema, "", "  ")
	require.NoError(t, err)

	schemaStr := string(data)

	// Should contain connection-level fields
	assert.Contains(t, schemaStr, "connections")
	assert.Contains(t, schemaStr, "parallelism")
	assert.Contains(t, schemaStr, "disabled")

	// Should contain all warehouse types
	for _, wh := range []string{
		"postgres", "snowflake", "bigquery", "redshift", "mysql",
		"clickhouse", "trino", "databricks", "mssql", "oracle", "duckdb",
	} {
		assert.Contains(t, schemaStr, wh, "schema should contain %s", wh)
	}
}

func TestNewReflector(t *testing.T) {
	r := NewReflector()
	require.NotNil(t, r)
	assert.Equal(t, "yaml", r.FieldNameTag)
	assert.True(t, r.ExpandedStruct)
}

func TestConnectionsSchema_CanBeComposed(t *testing.T) {
	// Verify that Connection type can be reflected as part of a larger struct
	type AppConfig struct {
		AgentName   string                 `yaml:"agent_name"`
		Connections map[string]*Connection `yaml:"connections" jsonschema:"required,minProperties=1"`
	}

	r := NewReflector()
	schema := r.Reflect(&AppConfig{})
	require.NotNil(t, schema)

	data, err := json.MarshalIndent(schema, "", "  ")
	require.NoError(t, err)

	schemaStr := string(data)
	assert.Contains(t, schemaStr, "agent_name")
	assert.Contains(t, schemaStr, "connections")
	assert.Contains(t, schemaStr, "postgres")
}
