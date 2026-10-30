package snowflake

import (
	"context"
	"fmt"
	"strings"

	dwhexecsnowflake "github.com/getsynq/dwhsupport/exec/snowflake"
	"github.com/getsynq/dwhsupport/logging"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scope"
	"github.com/getsynq/dwhsupport/sqldialect"
	"github.com/pkg/errors"
	"github.com/samber/lo"
)

// The queries below are the CatalogFromAccountUsage reads. Each one reads all
// databases at once: %[1]s is the database holding ACCOUNT_USAGE and %[2]s the
// quoted names of the databases to read. ACCOUNT_USAGE keeps a dropped object
// with its DELETED time, and an object dropped and created again under the same
// name is a second row, so every query keeps only live objects and the newest
// object of a name.

var accountUsageDatabasesQuery = `
SELECT database_name AS "name",
       '' AS "origin",
       NVL(database_owner, '') AS "owner",
       NVL(comment, '') AS "comment",
       NVL(type, '') AS "kind"
FROM %s.ACCOUNT_USAGE.DATABASES
WHERE deleted IS NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY database_name ORDER BY created DESC) = 1
`

var accountUsageSchemasQuery = `
SELECT s.catalog_name AS "database",
       s.schema_name AS "schema",
       NVL(s.comment, '') AS "description",
       s.schema_owner AS "schema_owner"
FROM %[1]s.ACCOUNT_USAGE.SCHEMATA AS s
WHERE s.deleted IS NULL
  AND s.catalog_name IN (%[2]s)
  AND UPPER(s.schema_name) NOT IN ('INFORMATION_SCHEMA')
  /* SYNQ_SCOPE_FILTER */
QUALIFY ROW_NUMBER() OVER (PARTITION BY s.catalog_name, s.schema_name ORDER BY s.created DESC) = 1
`

var accountUsageTablesQuery = `
SELECT t.table_catalog AS "database",
       t.table_schema AS "schema",
       t.table_name AS "table",
       IFF(t.is_dynamic = 'YES', 'DYNAMIC TABLE', t.table_type) AS "table_type",
       NVL(t.comment, '') AS "description",
       (t.table_type = 'VIEW' OR t.table_type = 'MATERIALIZED VIEW') AS "is_view",
       t.table_type = 'BASE TABLE' AS "is_table",
       t.table_owner AS "owner",
       t.created AS "created_at"
FROM %[1]s.ACCOUNT_USAGE.TABLES AS t
WHERE t.deleted IS NULL
  AND t.table_catalog IN (%[2]s)
  AND UPPER(t.table_schema) NOT IN ('INFORMATION_SCHEMA', 'SYSADMIN')
  /* SYNQ_SCOPE_FILTER */
QUALIFY ROW_NUMBER() OVER (PARTITION BY t.table_catalog, t.table_schema, t.table_name ORDER BY t.created DESC) = 1
`

var accountUsageCatalogQuery = `
WITH live_tables AS (
    SELECT t.table_id, t.table_catalog, t.table_schema, t.table_name, t.table_type
    FROM %[1]s.ACCOUNT_USAGE.TABLES AS t
    WHERE t.deleted IS NULL
      AND t.table_catalog IN (%[2]s)
      AND UPPER(t.table_schema) NOT IN ('INFORMATION_SCHEMA', 'SYSADMIN')
      /* SYNQ_SCOPE_FILTER */
    QUALIFY ROW_NUMBER() OVER (PARTITION BY t.table_catalog, t.table_schema, t.table_name ORDER BY t.created DESC) = 1
)
SELECT CURRENT_ORGANIZATION_NAME() || '-' || CURRENT_ACCOUNT() AS "instance",
       t.table_catalog AS "database",
       t.table_schema AS "schema",
       t.table_name AS "table",
       COALESCE((t.table_type = 'MATERIALIZED VIEW' OR t.table_type = 'VIEW'), FALSE) AS "is_view",
       c.column_name AS "column",
       c.data_type AS "type",
       c.numeric_precision AS "numeric_precision",
       c.numeric_scale AS "numeric_scale",
       c.is_nullable = 'YES' AS "is_nullable",
       c.comment AS "comment",
       c.ordinal_position AS "position"
FROM %[1]s.ACCOUNT_USAGE.COLUMNS AS c
JOIN live_tables AS t ON c.table_id = t.table_id
WHERE c.deleted IS NULL
  -- Snowflake's own hidden columns (row versions of change tracking, a dynamic
  -- table's row ids), which INFORMATION_SCHEMA leaves out.
  AND NOT STARTSWITH(c.column_name, 'METADATA$')
QUALIFY ROW_NUMBER() OVER (PARTITION BY c.table_id, c.column_name ORDER BY c.ordinal_position) = 1
`

var accountUsageTableMetricsQuery = `
SELECT t.table_catalog AS "database",
       t.table_schema AS "schema",
       t.table_name AS "table",
       t.row_count AS "row_count",
       t.bytes AS "size_bytes",
       t.last_altered AS "updated_at"
FROM %[1]s.ACCOUNT_USAGE.TABLES AS t
WHERE t.deleted IS NULL
  AND t.row_count IS NOT NULL
  -- ACCOUNT_USAGE counts 0 rows for a view, where INFORMATION_SCHEMA has none.
  AND t.table_type != 'VIEW'
  AND t.table_catalog IN (%[2]s)
  AND t.table_schema NOT IN ('INFORMATION_SCHEMA')
  /* SYNQ_SCOPE_FILTER */
QUALIFY ROW_NUMBER() OVER (PARTITION BY t.table_catalog, t.table_schema, t.table_name ORDER BY t.created DESC) = 1
`

// accountUsageSqlDefinitionsQuery reads the view definitions, and lists the
// other tables with no SQL, as the INFORMATION_SCHEMA read does. The two parts
// are filtered on their own, so the scope filter is in both.
var accountUsageSqlDefinitionsQuery = `
SELECT v.table_catalog AS "database",
       v.table_schema AS "schema",
       v.table_name AS "table",
       TRUE AS "is_view",
       NVL(v.view_definition, '') AS "sql"
FROM %[1]s.ACCOUNT_USAGE.VIEWS AS v
WHERE v.deleted IS NULL
  AND v.table_catalog IN (%[2]s)
  AND UPPER(v.table_schema) NOT IN ('INFORMATION_SCHEMA', 'SYSADMIN')
  /* SYNQ_SCOPE_FILTER */
QUALIFY ROW_NUMBER() OVER (PARTITION BY v.table_catalog, v.table_schema, v.table_name ORDER BY v.created DESC) = 1
UNION ALL
SELECT t.table_catalog AS "database",
       t.table_schema AS "schema",
       t.table_name AS "table",
       FALSE AS "is_view",
       '' AS "sql"
FROM %[1]s.ACCOUNT_USAGE.TABLES AS t
WHERE t.deleted IS NULL
  AND t.table_catalog IN (%[2]s)
  AND UPPER(t.table_schema) NOT IN ('INFORMATION_SCHEMA', 'SYSADMIN')
  AND t.table_type != 'VIEW'
  AND t.table_type != 'MATERIALIZED VIEW'
  /* SYNQ_SCOPE_FILTER */
QUALIFY ROW_NUMBER() OVER (PARTITION BY t.table_catalog, t.table_schema, t.table_name ORDER BY t.created DESC) = 1
`

// accountUsageDatabases lists the live databases of the account. A database
// Snowflake itself provides (the SNOWFLAKE application) and a user's personal
// database are not listed, as they hold nothing to observe.
func accountUsageDatabases(
	ctx context.Context,
	executor *dwhexecsnowflake.SnowflakeExecutor,
	accountUsageDb string,
) ([]*DbDesc, error) {
	var databases []*DbDesc
	if err := executor.Select(ctx, &databases, fmt.Sprintf(accountUsageDatabasesQuery, accountUsageDb)); err != nil {
		return nil, errors.Wrapf(
			err,
			"failed to list databases from %s.ACCOUNT_USAGE.DATABASES, the role needs IMPORTED PRIVILEGES on the %s database",
			accountUsageDb, accountUsageDb,
		)
	}
	return lo.Filter(databases, func(db *DbDesc, _ int) bool {
		return db.Kind != "APPLICATION" && db.Kind != "PERSONAL DATABASE"
	}), nil
}

// accountUsageQuery fills in a CatalogFromAccountUsage query for the
// databases the account owns and the scope filter. It answers false when
// there is no database to read.
func (e *SnowflakeScrapper) accountUsageQuery(ctx context.Context, query, dbCol, schemaCol, tableCol string) (string, bool, error) {
	databases, _, err := e.splitShares(ctx)
	if err != nil {
		return "", false, err
	}
	if len(databases) == 0 {
		return "", false, nil
	}
	dialect := sqldialect.NewSnowflakeDialect()
	names := lo.Map(databases, func(database string, _ int) string { return dialect.StringLiteral(database) })
	query = fmt.Sprintf(query, e.accountUsageDb(), strings.Join(names, ", "))
	if tableCol == "" {
		return scope.AppendSchemaScopeConditions(ctx, query, dbCol, schemaCol), true, nil
	}
	return scope.AppendScopeConditions(ctx, query, dbCol, schemaCol, tableCol), true, nil
}

// selectAccountUsage runs a CatalogFromAccountUsage query and scans its rows.
func selectAccountUsage[T any](ctx context.Context, e *SnowflakeScrapper, query, dbCol, schemaCol, tableCol, source string) ([]*T, error) {
	sql, ok, err := e.accountUsageQuery(ctx, query, dbCol, schemaCol, tableCol)
	if err != nil || !ok {
		return nil, err
	}
	rows, err := e.executor.QueryRows(ctx, sql)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to query %s.ACCOUNT_USAGE.%s", e.accountUsageDb(), source)
	}
	defer rows.Close()
	return scrapper.ScanAll[T](ctx, rows, fmt.Sprintf("%s.ACCOUNT_USAGE.%s", e.accountUsageDb(), source))
}

// streamsWithPrivileges lists the streams of the databases the account owns
// and the role holds a privilege on, as SHOW STREAMS lists only those. A
// database SHOW STREAMS fails on is left out. A share gets its streams with
// the rest of its objects.
func (e *SnowflakeScrapper) streamsWithPrivileges(ctx context.Context) ([]*ShowStreamsRow, error) {
	databases, err := e.getDatabasesWithPrivileges(ctx)
	if err != nil {
		return nil, err
	}
	shares, err := e.importedShares(ctx)
	if err != nil {
		return nil, err
	}
	var out []*ShowStreamsRow
	for _, database := range databases {
		if shares[database] {
			continue
		}
		streams, err := e.showStreamsInDatabase(ctx, database)
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return nil, ctxErr
			}
			logging.GetLogger(ctx).WithField("database", database).WithError(err).Warn("SHOW STREAMS IN DATABASE failed")
			continue
		}
		out = append(out, streams...)
	}
	return out, nil
}

func (e *SnowflakeScrapper) querySchemasFromAccountUsage(ctx context.Context) ([]*scrapper.SchemaRow, error) {
	rows, err := selectAccountUsage[scrapper.SchemaRow](ctx, e, accountUsageSchemasQuery, "s.catalog_name", "s.schema_name", "", "SCHEMATA")
	if err != nil {
		return nil, err
	}
	for _, row := range rows {
		row.Instance = e.conf.Account
		if row.Description != nil && *row.Description == "" {
			row.Description = nil
		}
	}
	return withShares(ctx, e, rows, e.querySchemasIn)
}

// withShares adds to the rows read from ACCOUNT_USAGE the rows of the shares
// the role can use, which readIn reads from their own INFORMATION_SCHEMA. An
// incomplete read of the shares goes back with the rows.
func withShares[T any](
	ctx context.Context,
	e *SnowflakeScrapper,
	rows []*T,
	readIn func(context.Context, []string) ([]*T, error),
) ([]*T, error) {
	_, shares, err := e.splitShares(ctx)
	if err != nil {
		return nil, err
	}
	if len(shares) == 0 {
		return rows, nil
	}
	shareRows, incomplete := readIn(ctx, shares)
	if scrapper.Failed(incomplete) {
		return nil, incomplete
	}
	return append(rows, shareRows...), incomplete
}

func (e *SnowflakeScrapper) queryTablesFromAccountUsage(ctx context.Context) ([]*scrapper.TableRow, error) {
	rows, err := selectAccountUsage[scrapper.TableRow](
		ctx, e, accountUsageTablesQuery, "t.table_catalog", "t.table_schema", "t.table_name", "TABLES",
	)
	if err != nil {
		return nil, err
	}
	for _, row := range rows {
		row.Instance = e.conf.Account
		if row.Description != nil && *row.Description == "" {
			row.Description = nil
		}
		row.Owner = lo.EmptyableToPtr(lo.FromPtr(row.Owner))
		row.CreatedAt = scrapper.UTCTimePtr(row.CreatedAt)
	}
	streams, err := e.streamsWithPrivileges(ctx)
	if err != nil {
		return nil, err
	}
	rows = scope.FilterRows(append(rows, e.streamTableRows(streams)...), scope.GetScope(ctx))
	return withShares(ctx, e, rows, e.queryTablesIn)
}

func (e *SnowflakeScrapper) queryCatalogFromAccountUsage(ctx context.Context) ([]*scrapper.CatalogColumnRow, error) {
	rows, err := selectAccountUsage[scrapper.CatalogColumnRow](
		ctx, e, accountUsageCatalogQuery, "t.table_catalog", "t.table_schema", "t.table_name", "COLUMNS",
	)
	if err != nil {
		return nil, err
	}
	for _, row := range rows {
		row.Instance = e.conf.Account
		row.ResolveNumeric(e.DialectType())
	}
	rows, incomplete := withShares(ctx, e, rows, e.queryCatalogIn)
	if scrapper.Failed(incomplete) {
		return nil, incomplete
	}
	streams, err := e.streamsWithPrivileges(ctx)
	if err != nil {
		return nil, err
	}
	rows = append(rows, streamCatalogRows(ctx, streams, rows)...)
	databases, err := e.GetDatabasesToQuery(ctx)
	if err != nil {
		return nil, err
	}
	if err := e.addTagReferences(ctx, databases, rows); err != nil {
		return nil, err
	}
	return scope.FilterRows(rows, scope.GetScope(ctx)), incomplete
}

func (e *SnowflakeScrapper) queryTableMetricsFromAccountUsage(ctx context.Context) ([]*scrapper.TableMetricsRow, error) {
	rows, err := selectAccountUsage[scrapper.TableMetricsRow](
		ctx, e, accountUsageTableMetricsQuery, "t.table_catalog", "t.table_schema", "t.table_name", "TABLES",
	)
	if err != nil {
		return nil, err
	}
	for _, row := range rows {
		row.Instance = e.conf.Account
		row.UpdatedAt = scrapper.UTCTimePtr(row.UpdatedAt)
	}
	return withShares(ctx, e, rows, e.queryTableMetricsIn)
}

// querySqlDefinitionsFromAccountUsage reads the view definitions from
// ACCOUNT_USAGE. GET_DDL answers only for the databases the role holds a
// privilege on, so only there a table, a dynamic table or a materialized view
// gets its DDL and an object its tags.
func (e *SnowflakeScrapper) querySqlDefinitionsFromAccountUsage(ctx context.Context) ([]*scrapper.SqlDefinitionRow, error) {
	rows, err := selectAccountUsage[scrapper.SqlDefinitionRow](
		ctx, e, accountUsageSqlDefinitionsQuery, "table_catalog", "table_schema", "table_name", "VIEWS",
	)
	if err != nil {
		return nil, err
	}
	for _, row := range rows {
		row.Instance = e.conf.Account
	}
	streams, err := e.streamsWithPrivileges(ctx)
	if err != nil {
		return nil, err
	}
	rows = scope.FilterRows(append(rows, e.streamSqlDefinitionRows(streams)...), scope.GetScope(ctx))
	rows, incomplete := withShares(ctx, e, rows, e.querySqlDefinitionsIn)
	if scrapper.Failed(incomplete) {
		return nil, incomplete
	}

	withPrivileges, err := e.getDatabasesWithPrivileges(ctx)
	if err != nil {
		return nil, err
	}
	readable := lo.SliceToMap(withPrivileges, func(database string) (string, bool) { return database, true })
	if err := e.addDdls(ctx, lo.Filter(rows, func(row *scrapper.SqlDefinitionRow, _ int) bool { return readable[row.Database] })); err != nil {
		return nil, err
	}
	return rows, incomplete
}
