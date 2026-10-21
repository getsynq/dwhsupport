package redshift

import "strings"

// redshiftSystemSchemas are the schemas Redshift creates for itself in every
// database. pg_auto_copy (auto-copy jobs), pg_mv and pg_s3 are owned by rdsdb
// like the rest, and their views are readable by every user.
//
// Objects are not left out by their owner alone: public belongs to rdsdb too,
// and so may the tables a zero-ETL integration replicates, which are the
// customer's data. Inside a customer's schema only the materialized view's
// backing table is left out, see each query's mv_tbl__ condition.
var redshiftSystemSchemas = []string{
	"pg_catalog", "information_schema", "catalog_history", "pg_internal", "pg_toast",
	"pg_automv", "pg_auto_copy", "pg_mv", "pg_s3",
}

// systemSchemasPlaceholder is where a query lists the system schemas, as the
// contents of a NOT IN (...).
const systemSchemasPlaceholder = "{{system_schemas}}"

// withSystemSchemas fills a query's list of the schemas it leaves out.
func withSystemSchemas(sql string) string {
	quoted := make([]string, len(redshiftSystemSchemas))
	for i, schema := range redshiftSystemSchemas {
		quoted[i] = "'" + schema + "'"
	}
	return strings.ReplaceAll(sql, systemSchemasPlaceholder, strings.Join(quoted, ", "))
}
