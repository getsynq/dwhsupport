package trino

import (
	"slices"
	"strings"
)

// excludedSchemasPlaceholder is where a catalog query lists the schemas it leaves
// out, as the contents of a NOT IN (...).
const excludedSchemasPlaceholder = "{{excluded_schemas}}"

// connectorSystemSchemas are the schemas the database behind a connector keeps
// for itself, by connector name (system.metadata.catalogs.connector_name). Trino
// lists them as ordinary schemas of the catalog, so without this a catalog over
// SQL Server publishes every view of sys as a table. Each list is what the
// scrapper for that database skips natively, in lower case, which is how Trino
// reports every name.
var connectorSystemSchemas = map[string][]string{
	// The fixed database roles each own a schema of their name.
	"sqlserver": {
		"sys", "guest", "cdc",
		"db_accessadmin", "db_backupoperator", "db_datareader", "db_datawriter", "db_ddladmin",
		"db_denydatareader", "db_denydatawriter", "db_owner", "db_securityadmin",
	},
	// The users Oracle maintains (ALL_USERS.ORACLE_MAINTAINED = 'Y'), across 12c
	// to 23ai. Trino reaches Oracle without access to ALL_USERS, so the list is
	// fixed here rather than read.
	"oracle": {
		"anonymous", "appqossys", "audsys", "baassys", "ctxsys", "dbsfwuser", "dbsnmp", "dgpdb_int",
		"dip", "dvf", "dvsys", "flows_files", "ggsharedcap", "ggsys", "gsmadmin_internal", "gsmcatuser",
		"gsmrootuser", "gsmuser", "lbacsys", "mddata", "mdsys", "ojvmsys", "olapsys", "oracle_ocm",
		"orddata", "ordplugins", "ordsys", "outln", "rasadm", "remote_scheduler_agent",
		"si_informtn_schema", "spatial_csw_admin_usr", "spatial_wfs_admin_usr", "sys", "sys$umf",
		"sysbackup", "sysdg", "syskm", "sysrac", "system", "vecsys", "wmsys", "xdb", "xs$null",
	},
	"postgresql": {"pg_catalog", "pg_toast"},
	"redshift": {
		"pg_catalog", "pg_toast", "pg_internal", "catalog_history",
		"pg_automv", "pg_auto_copy", "pg_mv", "pg_s3",
	},
	"mysql":      {"mysql", "sys", "performance_schema"},
	"mariadb":    {"mysql", "sys", "performance_schema"},
	"clickhouse": {"system"},
	// An Iceberg JDBC catalog lists its own iceberg_tables under system.
	"iceberg": {"system"},
}

// excludedSchemas is the SQL list of the schemas a catalog query over a catalog
// of connectorName leaves out: information_schema, which Trino adds to every
// catalog, and the connector's system schemas.
func excludedSchemas(connectorName string) string {
	schemas := slices.Concat([]string{"information_schema"}, connectorSystemSchemas[connectorName])
	quoted := make([]string, len(schemas))
	for i, schema := range schemas {
		quoted[i] = "'" + strings.ReplaceAll(schema, "'", "''") + "'"
	}
	return strings.Join(quoted, ", ")
}

// withCatalog fills a catalog query's catalog name and the schemas it leaves out.
func withCatalog(query string, catalog *TrinoCatalog) string {
	query = strings.ReplaceAll(query, "{{catalog}}", catalog.CatalogName)
	return strings.ReplaceAll(query, excludedSchemasPlaceholder, excludedSchemas(catalog.ConnectorName))
}
