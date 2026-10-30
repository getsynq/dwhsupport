package snowflake

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	dwhexecsnowflake "github.com/getsynq/dwhsupport/exec/snowflake"
	"github.com/getsynq/dwhsupport/logging"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scope"
	"github.com/getsynq/dwhsupport/sqldialect"
	"github.com/pkg/errors"
	"github.com/samber/lo"
	"golang.org/x/sync/errgroup"
)

// The queries below are the CatalogFromAccountUsage reads. Each one reads all
// databases at once: %[1]s is the database holding ACCOUNT_USAGE and %[2]s the
// quoted names of the databases to read. ACCOUNT_USAGE keeps a dropped object
// with its DELETED time, so every query keeps only live objects. An object
// replaced under the same name can still be listed twice, as Snowflake does not
// always mark the replaced one deleted. Only the small listings (databases,
// schemas) keep the newest of a name in SQL. The large ones return the
// creation time and newestByName keeps the newest in Go, as a window over a
// whole ACCOUNT_USAGE view costs minutes on a large account.

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
`

// accountUsageCatalogQuery joins the columns to their table by id, so a column
// of a replaced table goes with that table, and newestByName drops both.
var accountUsageCatalogQuery = `
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
       c.ordinal_position AS "position",
       t.table_id AS "table_id",
       t.created AS "table_created"
FROM %[1]s.ACCOUNT_USAGE.COLUMNS AS c
JOIN %[1]s.ACCOUNT_USAGE.TABLES AS t ON c.table_id = t.table_id
WHERE c.deleted IS NULL
  AND t.deleted IS NULL
  AND c.table_catalog IN (%[2]s)
  AND t.table_catalog IN (%[2]s)
  AND UPPER(t.table_schema) NOT IN ('INFORMATION_SCHEMA', 'SYSADMIN')
  -- Snowflake's own hidden columns (row versions of change tracking, a dynamic
  -- table's row ids), which INFORMATION_SCHEMA leaves out.
  AND NOT STARTSWITH(c.column_name, 'METADATA$')
  /* SYNQ_SCOPE_FILTER */
`

var accountUsageTableMetricsQuery = `
SELECT t.table_catalog AS "database",
       t.table_schema AS "schema",
       t.table_name AS "table",
       t.row_count AS "row_count",
       t.bytes AS "size_bytes",
       t.last_altered AS "updated_at",
       t.created AS "created"
FROM %[1]s.ACCOUNT_USAGE.TABLES AS t
WHERE t.deleted IS NULL
  AND t.row_count IS NOT NULL
  -- ACCOUNT_USAGE counts 0 rows for a view, where INFORMATION_SCHEMA has none.
  AND t.table_type != 'VIEW'
  AND t.table_catalog IN (%[2]s)
  AND t.table_schema NOT IN ('INFORMATION_SCHEMA')
  /* SYNQ_SCOPE_FILTER */
`

// accountUsageSqlDefinitionsQuery reads the view definitions, and lists the
// other tables with no SQL, as the INFORMATION_SCHEMA read does. The two parts
// are filtered on their own, so the scope filter is in both.
var accountUsageSqlDefinitionsQuery = `
SELECT v.table_catalog AS "database",
       v.table_schema AS "schema",
       v.table_name AS "table",
       TRUE AS "is_view",
       NVL(v.view_definition, '') AS "sql",
       v.created AS "created"
FROM %[1]s.ACCOUNT_USAGE.VIEWS AS v
WHERE v.deleted IS NULL
  AND v.table_catalog IN (%[2]s)
  AND UPPER(v.table_schema) NOT IN ('INFORMATION_SCHEMA', 'SYSADMIN')
  /* SYNQ_SCOPE_FILTER */
UNION ALL
SELECT t.table_catalog AS "database",
       t.table_schema AS "schema",
       t.table_name AS "table",
       FALSE AS "is_view",
       '' AS "sql",
       t.created AS "created"
FROM %[1]s.ACCOUNT_USAGE.TABLES AS t
WHERE t.deleted IS NULL
  AND t.table_catalog IN (%[2]s)
  AND UPPER(t.table_schema) NOT IN ('INFORMATION_SCHEMA', 'SYSADMIN')
  AND t.table_type != 'VIEW'
  AND t.table_type != 'MATERIALIZED VIEW'
  /* SYNQ_SCOPE_FILTER */
`

// accountUsageColumnRow is a COLUMNS row with the id and creation time of its
// table, which newestByName and the column dedup read.
type accountUsageColumnRow struct {
	scrapper.CatalogColumnRow
	TableId      int64      `db:"table_id"`
	TableCreated *time.Time `db:"table_created"`
}

// accountUsageMetricsRow is a TABLES metrics row with its creation time.
type accountUsageMetricsRow struct {
	scrapper.TableMetricsRow
	Created *time.Time `db:"created"`
}

// accountUsageSqlDefinitionRow is a VIEWS or TABLES row with its creation time.
type accountUsageSqlDefinitionRow struct {
	scrapper.SqlDefinitionRow
	Created *time.Time `db:"created"`
}

type objectName struct {
	database, schema, table string
}

// newestByName keeps, for every name, the rows of the object created last,
// in the order the rows came. An object is the rows of one id; a read with
// one row per object passes the row's index as its id.
func newestByName[T any](rows []T, nameOf func(T) objectName, idOf func(int, T) int64, createdOf func(T) *time.Time) []T {
	type newest struct {
		id      int64
		created time.Time
	}
	best := map[objectName]newest{}
	for i, row := range rows {
		name := nameOf(row)
		created := lo.FromPtr(createdOf(row))
		current, found := best[name]
		if !found || created.After(current.created) {
			best[name] = newest{id: idOf(i, row), created: created}
		}
	}
	return lo.Filter(rows, func(row T, i int) bool { return best[nameOf(row)].id == idOf(i, row) })
}

func rowIndex[T any](i int, _ T) int64 { return int64(i) }

// firstPositionOfEachColumn keeps one row per column of a table, the one at
// the lowest position, in the order the rows came.
func firstPositionOfEachColumn(columns []*accountUsageColumnRow) []*scrapper.CatalogColumnRow {
	type columnKey struct {
		tableId int64
		column  string
	}
	first := map[columnKey]*accountUsageColumnRow{}
	for _, column := range columns {
		key := columnKey{column.TableId, column.Column}
		if kept, found := first[key]; !found || column.Position < kept.Position {
			first[key] = column
		}
	}
	rows := make([]*scrapper.CatalogColumnRow, 0, len(first))
	for _, column := range columns {
		if first[columnKey{column.TableId, column.Column}] == column {
			rows = append(rows, &column.CatalogColumnRow)
		}
	}
	return rows
}

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
	databases, err := e.DatabasesWithPrivileges(ctx)
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
	rows = newestByName(rows,
		func(row *scrapper.TableRow) objectName { return objectName{row.Database, row.Schema, row.Table} },
		rowIndex[*scrapper.TableRow],
		func(row *scrapper.TableRow) *time.Time { return row.CreatedAt },
	)
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
	databases, err := e.GetDatabasesToQuery(ctx)
	if err != nil {
		return nil, err
	}
	// TAG_REFERENCES is as slow to read as COLUMNS on a large account, so the
	// two are read at the same time.
	var references *tagReferences
	var referencesErr error
	referencesRead := make(chan struct{})
	go func() {
		defer close(referencesRead)
		references, referencesErr = e.queryTagReferences(ctx, databases)
	}()

	columns, err := selectAccountUsage[accountUsageColumnRow](
		ctx, e, accountUsageCatalogQuery, "t.table_catalog", "t.table_schema", "t.table_name", "COLUMNS",
	)
	<-referencesRead
	if err != nil {
		return nil, err
	}
	columns = newestByName(columns,
		func(row *accountUsageColumnRow) objectName { return objectName{row.Database, row.Schema, row.Table} },
		func(_ int, row *accountUsageColumnRow) int64 { return row.TableId },
		func(row *accountUsageColumnRow) *time.Time { return row.TableCreated },
	)
	rows := firstPositionOfEachColumn(columns)
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
	if err := e.applyTagReferences(ctx, references, referencesErr, rows); err != nil {
		return nil, err
	}
	return scope.FilterRows(rows, scope.GetScope(ctx)), incomplete
}

func (e *SnowflakeScrapper) queryTableMetricsFromAccountUsage(ctx context.Context) ([]*scrapper.TableMetricsRow, error) {
	read, err := selectAccountUsage[accountUsageMetricsRow](
		ctx, e, accountUsageTableMetricsQuery, "t.table_catalog", "t.table_schema", "t.table_name", "TABLES",
	)
	if err != nil {
		return nil, err
	}
	read = newestByName(read,
		func(row *accountUsageMetricsRow) objectName { return objectName{row.Database, row.Schema, row.Table} },
		rowIndex[*accountUsageMetricsRow],
		func(row *accountUsageMetricsRow) *time.Time { return row.Created },
	)
	rows := make([]*scrapper.TableMetricsRow, 0, len(read))
	for _, r := range read {
		row := &r.TableMetricsRow
		row.Instance = e.conf.Account
		row.UpdatedAt = scrapper.UTCTimePtr(row.UpdatedAt)
		rows = append(rows, row)
	}
	return withShares(ctx, e, rows, e.queryTableMetricsIn)
}

// querySqlDefinitionsFromAccountUsage reads the view definitions from
// ACCOUNT_USAGE. GET_DDL answers only for the databases the role holds a
// privilege on, so only there a table, a dynamic table or a materialized view
// gets its DDL and an object its tags.
func (e *SnowflakeScrapper) querySqlDefinitionsFromAccountUsage(ctx context.Context) ([]*scrapper.SqlDefinitionRow, error) {
	read, err := selectAccountUsage[accountUsageSqlDefinitionRow](
		ctx, e, accountUsageSqlDefinitionsQuery, "table_catalog", "table_schema", "table_name", "VIEWS",
	)
	if err != nil {
		return nil, err
	}
	read = newestByName(read,
		func(row *accountUsageSqlDefinitionRow) objectName {
			return objectName{row.Database, row.Schema, row.Table}
		},
		rowIndex[*accountUsageSqlDefinitionRow],
		func(row *accountUsageSqlDefinitionRow) *time.Time { return row.Created },
	)
	rows := make([]*scrapper.SqlDefinitionRow, 0, len(read))
	for _, r := range read {
		row := &r.SqlDefinitionRow
		row.Instance = e.conf.Account
		rows = append(rows, row)
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

	withPrivileges, err := e.DatabasesWithPrivileges(ctx)
	if err != nil {
		return nil, err
	}
	readable, err := e.schemasWithPrivileges(ctx, withPrivileges)
	if err != nil {
		return nil, err
	}
	if err := e.addDdls(ctx, lo.Filter(rows, func(row *scrapper.SqlDefinitionRow, _ int) bool {
		return readable[DatabaseAndSchema{DatabaseName: row.Database, SchemaName: row.Schema}]
	})); err != nil {
		return nil, err
	}
	return rows, incomplete
}

// schemasWithPrivileges lists the schemas of the databases that the role holds
// a privilege on, as SHOW SCHEMAS lists only those. GET_DDL of any other
// schema fails with "does not exist or not authorized", and a role reading
// ACCOUNT_USAGE often has USAGE on a database but not on all its schemas. A
// database SHOW SCHEMAS fails on is left out.
func (e *SnowflakeScrapper) schemasWithPrivileges(ctx context.Context, databases []string) (map[DatabaseAndSchema]bool, error) {
	var mu sync.Mutex
	schemas := map[DatabaseAndSchema]bool{}
	g, groupCtx := errgroup.WithContext(ctx)
	g.SetLimit(8)
	for _, database := range databases {
		g.Go(func() error {
			var shown []string
			rows, err := e.executor.QueryRows(groupCtx, fmt.Sprintf("SHOW TERSE SCHEMAS IN DATABASE %s", databaseIdent(database)))
			if err == nil {
				defer rows.Close()
				for rows.Next() {
					row := map[string]any{}
					if err = rows.MapScan(row); err != nil {
						break
					}
					shown = append(shown, mapStr(row, "name"))
				}
				if err == nil {
					err = rows.Err()
				}
			}
			if err != nil {
				if ctxErr := groupCtx.Err(); ctxErr != nil {
					return ctxErr
				}
				logging.GetLogger(ctx).WithField("database", database).WithError(err).Warn("SHOW SCHEMAS IN DATABASE failed")
				return nil
			}
			mu.Lock()
			defer mu.Unlock()
			for _, schema := range shown {
				schemas[DatabaseAndSchema{DatabaseName: database, SchemaName: schema}] = true
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return schemas, nil
}
