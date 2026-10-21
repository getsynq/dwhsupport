package oracle

import (
	"context"
	_ "embed"
	"sort"
	"strings"

	dwhexec "github.com/getsynq/dwhsupport/exec"
	dwhexecoracle "github.com/getsynq/dwhsupport/exec/oracle"
	"github.com/getsynq/dwhsupport/logging"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scope"
	"github.com/pkg/errors"
)

//go:embed query_sql_definitions.sql
var querySqlDefinitionsSql string

//go:embed query_table_ddl_tables.sql
var queryTableDdlTablesSql string

//go:embed query_table_ddl.sql
var queryTableDdlSql string

//go:embed query_table_ddl_session.sql
var queryTableDdlSessionSql string

// getDdlParallelism is how many GET_DDL statements run at a time. DBMS_METADATA
// takes about 200 ms for a table, and much longer for the first table of a
// schema.
const getDdlParallelism = 4

// errNoCatalogRole is why a table of another schema has no DDL.
var errNoCatalogRole = errors.New(
	"DBMS_METADATA.GET_DDL shows another schema's tables only to a role with SELECT_CATALOG_ROLE " +
		"(SELECT ANY DICTIONARY is not enough), grant it to read their DDL")

func (e *OracleScrapper) QuerySqlDefinitions(ctx context.Context) ([]*scrapper.SqlDefinitionRow, error) {
	sql := scope.AppendScopeConditions(ctx, querySqlDefinitionsSql, "", "v.OWNER", "v.VIEW_NAME")
	views, err := dwhexecoracle.NewQuerier[scrapper.SqlDefinitionRow](e.executor).QueryMany(ctx, sql,
		dwhexec.WithPostProcessors(func(row *scrapper.SqlDefinitionRow) (*scrapper.SqlDefinitionRow, error) {
			row.Database = e.conf.ServiceName
			row.Instance = e.conf.Host
			return row, nil
		}),
	)
	if err != nil {
		return nil, err
	}

	tables, omissions, err := e.queryTableDdls(ctx)
	if err != nil {
		return nil, err
	}
	return append(views, tables...), scrapper.Incomplete(omissions)
}

type tableName struct {
	Schema string `db:"schema"`
	Table  string `db:"table"`
}

type tableDdlSession struct {
	User        string `db:"user"`
	CatalogRole int    `db:"catalog_role"`
}

// queryTableDdls reads the DDL of every table in scope with
// DBMS_METADATA.GET_DDL. GET_DDL shows a table of another schema only to a
// role with SELECT_CATALOG_ROLE, and answers ORA-31603 for it otherwise, so
// without that role only the connected user's own tables are read and every
// other schema is left out whole.
func (e *OracleScrapper) queryTableDdls(ctx context.Context) ([]*scrapper.SqlDefinitionRow, []scrapper.Omission, error) {
	sql := scope.AppendScopeConditions(ctx, queryTableDdlTablesSql, "", "t.OWNER", "t.TABLE_NAME")
	names, err := dwhexecoracle.NewQuerier[tableName](e.executor).QueryMany(ctx, sql)
	if err != nil || len(names) == 0 {
		return nil, nil, err
	}
	tables := make([]*scrapper.SqlDefinitionRow, len(names))
	for i, name := range names {
		tables[i] = &scrapper.SqlDefinitionRow{
			Instance: e.conf.Host, Database: e.conf.ServiceName, Schema: name.Schema, Table: name.Table, IsTable: true,
		}
	}

	sessions, err := dwhexecoracle.NewQuerier[tableDdlSession](e.executor).QueryMany(ctx, queryTableDdlSessionSql)
	if err != nil {
		return nil, nil, err
	}
	if len(sessions) != 1 {
		return nil, nil, errors.Errorf("reading the session's user and roles returned %d rows", len(sessions))
	}
	readable, unreadableSchemas := splitReadableTables(tables, *sessions[0])

	var omissions []scrapper.Omission
	if len(unreadableSchemas) > 0 {
		if scrapper.IncompleteResultsAccepted(ctx) {
			for _, schema := range unreadableSchemas {
				omissions = append(omissions, scrapper.Omission{
					Database: e.conf.ServiceName, Schema: schema, Part: scrapper.OmittedDefinitions, Err: errNoCatalogRole,
				})
			}
		} else {
			logging.GetLogger(ctx).Warnf("No table DDL of %d schemas (%s): %v",
				len(unreadableSchemas), strings.Join(unreadableSchemas, ", "), errNoCatalogRole)
		}
	}

	rows, tableOmissions, err := scrapper.ReadTableDdls(ctx, readable, getDdlParallelism, e.getTableDdl, isObjectGone)
	if err != nil {
		return nil, nil, err
	}
	return rows, append(omissions, tableOmissions...), nil
}

// splitReadableTables keeps the tables GET_DDL shows to the session, and names
// the schemas whose tables it does not.
func splitReadableTables(tables []*scrapper.SqlDefinitionRow, session tableDdlSession) ([]*scrapper.SqlDefinitionRow, []string) {
	if session.CatalogRole > 0 {
		return tables, nil
	}
	var readable []*scrapper.SqlDefinitionRow
	unreadable := map[string]bool{}
	for _, table := range tables {
		if table.Schema == session.User {
			readable = append(readable, table)
		} else {
			unreadable[table.Schema] = true
		}
	}
	schemas := make([]string, 0, len(unreadable))
	for schema := range unreadable {
		schemas = append(schemas, schema)
	}
	sort.Strings(schemas)
	return readable, schemas
}

func (e *OracleScrapper) getTableDdl(ctx context.Context, table *scrapper.SqlDefinitionRow) (string, error) {
	var ddls []string
	if err := e.executor.Select(ctx, &ddls, queryTableDdlSql, table.Schema, table.Table); err != nil {
		return "", err
	}
	if len(ddls) == 0 {
		return "", nil
	}
	return strings.TrimSpace(ddls[0]), nil
}

// isObjectGone reports GET_DDL of a table dropped since it was listed. It is
// asked only for tables the session may see, so ORA-31603 (object not found)
// means the table is no longer there.
func isObjectGone(err error) bool {
	return strings.Contains(err.Error(), "ORA-31603")
}
