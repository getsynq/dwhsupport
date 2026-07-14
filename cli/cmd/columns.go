package cmd

import "github.com/getsynq/dwhsupport/cli/internal/output"

// Column definitions per row type. Default columns show the identifying fields
// a human usually wants; --wide (or -o json/yaml/toon) exposes everything.

var tableColumns = output.Columns{
	{Header: "database", Path: ".database", Default: true},
	{Header: "schema", Path: ".schema", Default: true},
	{Header: "table", Path: ".table", Default: true},
	{Header: "type", Path: ".table_type", Default: true},
	{Header: "is_view", Path: ".is_view", Default: false},
	{Header: "is_materialized_view", Path: ".is_materialized_view", Default: false},
	{Header: "description", Path: ".description", Default: false},
	{Header: "tags", Path: ".tags", Default: false},
}

var catalogColumnColumns = output.Columns{
	{Header: "database", Path: ".database", Default: true},
	{Header: "schema", Path: ".schema", Default: true},
	{Header: "table", Path: ".table", Default: true},
	{Header: "column", Path: ".column", Default: true},
	{Header: "type", Path: ".type", Default: true},
	{Header: "position", Path: ".position", Default: true},
	{Header: "comment", Path: ".comment", Default: false},
	{Header: "table_type", Path: ".table_type", Default: false},
	{Header: "column_tags", Path: ".column_tags", Default: false},
}

var tableMetricsColumns = output.Columns{
	{Header: "database", Path: ".database", Default: true},
	{Header: "schema", Path: ".schema", Default: true},
	{Header: "table", Path: ".table", Default: true},
	{Header: "row_count", Path: ".row_count", Default: true},
	{Header: "size_bytes", Path: ".size_bytes", Default: true},
	{Header: "updated_at", Path: ".updated_at", Default: true},
}

var schemaColumns = output.Columns{
	{Header: "database", Path: ".database", Default: true},
	{Header: "schema", Path: ".schema", Default: true},
	{Header: "type", Path: ".schema_type", Default: false},
	{Header: "owner", Path: ".schema_owner", Default: false},
	{Header: "description", Path: ".description", Default: false},
}

var databaseColumns = output.Columns{
	{Header: "database", Path: ".database", Default: true},
	{Header: "type", Path: ".database_type", Default: false},
	{Header: "owner", Path: ".database_owner", Default: false},
	{Header: "description", Path: ".description", Default: false},
}

var sqlDefinitionColumns = output.Columns{
	{Header: "database", Path: ".database", Default: true},
	{Header: "schema", Path: ".schema", Default: true},
	{Header: "table", Path: ".table", Default: true},
	{Header: "type", Path: ".table_type", Default: true},
	{Header: "sql", Path: ".sql", Default: false},
}

var constraintColumns = output.Columns{
	{Header: "database", Path: ".database", Default: true},
	{Header: "schema", Path: ".schema", Default: true},
	{Header: "table", Path: ".table", Default: true},
	{Header: "constraint_type", Path: ".constraint_type", Default: true},
	{Header: "column", Path: ".column_name", Default: true},
	{Header: "constraint_name", Path: ".constraint_name", Default: false},
	{Header: "expression", Path: ".constraint_expression", Default: false},
	{Header: "position", Path: ".column_position", Default: false},
	{Header: "is_enforced", Path: ".is_enforced", Default: false},
}

var shapeColumns = output.Columns{
	{Header: "position", Path: ".position", Default: true},
	{Header: "name", Path: ".name", Default: true},
	{Header: "native_type", Path: ".native_type", Default: true},
}
