package cmd

import (
	"github.com/getsynq/dwhsupport/cli/internal/output"
	"github.com/spf13/cobra"
)

// supportedDialects mirrors the connect.Connect switch. The example is the
// top-level config key plus a couple of the fields that identify the connection.
var supportedDialects = []map[string]any{
	{"dialect": "snowflake", "config_key": "snowflake", "example_fields": "account, username, password|private_key, warehouse"},
	{"dialect": "bigquery", "config_key": "bigquery", "example_fields": "project_id, service_account_key|service_account_key_file"},
	{"dialect": "databricks", "config_key": "databricks", "example_fields": "workspace_url, warehouse, auth_token|auth_client+auth_secret"},
	{"dialect": "postgres", "config_key": "postgres", "example_fields": "host, port, database, username, password"},
	{"dialect": "redshift", "config_key": "redshift", "example_fields": "host, port, database, username, password"},
	{"dialect": "clickhouse", "config_key": "clickhouse", "example_fields": "host, port, database, username, password"},
	{"dialect": "mysql", "config_key": "mysql", "example_fields": "host, port, database, username, password"},
	{"dialect": "trino", "config_key": "trino", "example_fields": "host, port, username, catalogs"},
	{"dialect": "oracle", "config_key": "oracle", "example_fields": "host, port, service_name, username, password"},
	{"dialect": "mssql", "config_key": "mssql", "example_fields": "host, port, database, username, password"},
	{"dialect": "athena", "config_key": "athena", "example_fields": "region, workgroup, catalog"},
	{"dialect": "fabric", "config_key": "fabric", "example_fields": "host, database, auth_type|client_id+client_secret+tenant_id"},
	{"dialect": "duckdb", "config_key": "duckdb", "example_fields": "path|motherduck_account (requires CGO build)"},
}

var dialectsCmd = &cobra.Command{
	Use:   "dialects",
	Short: "List the supported warehouse dialects and their config keys",
	Long: `List every warehouse this binary can connect to, with the top-level
config key to use and the fields that identify the connection. Use this to
discover what a connection config should look like.`,
	Example: "  dwhctl dialects\n  dwhctl dialects -o json",
	RunE: func(*cobra.Command, []string) error {
		return output.PrintList(supportedDialects, dialectColumns)
	},
}

var dialectColumns = output.Columns{
	{Header: "dialect", Path: ".dialect", Default: true},
	{Header: "config_key", Path: ".config_key", Default: true},
	{Header: "example_fields", Path: ".example_fields", Default: true},
}

func init() {
	rootCmd.AddCommand(dialectsCmd)
}
