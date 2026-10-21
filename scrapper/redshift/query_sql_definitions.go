package redshift

import (
	"context"
	_ "embed"
	"strings"

	dwhexec "github.com/getsynq/dwhsupport/exec"
	"github.com/getsynq/dwhsupport/exec/stdsql"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scope"
	"github.com/getsynq/dwhsupport/sqldialect"
	"github.com/lib/pq"
	"github.com/pkg/errors"
)

//go:embed query_sql_definitions.sql
var querySqlDefinitionsSql string

//go:embed query_table_ddl_tables.sql
var queryTableDdlTablesSql string

// showTableParallelism is how many SHOW TABLE statements run at a time. Each
// takes about 40 ms on a serverless workgroup.
const showTableParallelism = 8

func (e *RedshiftScrapper) QuerySqlDefinitions(ctx context.Context) ([]*scrapper.SqlDefinitionRow, error) {
	sql := scope.AppendScopeConditions(ctx, withSystemSchemas(querySqlDefinitionsSql), "", "table_schema", "table_name")
	views, err := stdsql.QueryMany[scrapper.SqlDefinitionRow](ctx, e.executor.GetDb(), sql,
		dwhexec.WithArgs[scrapper.SqlDefinitionRow](e.conf.Database),
		dwhexec.WithPostProcessors(func(row *scrapper.SqlDefinitionRow) (*scrapper.SqlDefinitionRow, error) {
			row.Database = e.conf.Database
			row.Instance = e.conf.Host
			return row, nil
		}),
	)
	if err != nil || !e.conf.UseShowCreateTable {
		return views, err
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

// queryTableDdls reads the DDL of every table in scope with SHOW TABLE.
func (e *RedshiftScrapper) queryTableDdls(ctx context.Context) ([]*scrapper.SqlDefinitionRow, []scrapper.Omission, error) {
	sql := scope.AppendScopeConditions(ctx, withSystemSchemas(queryTableDdlTablesSql), "", "n.nspname", "c.relname")
	names, err := stdsql.QueryMany[tableName](ctx, e.executor.GetDb(), sql)
	if err != nil {
		return nil, nil, err
	}
	tables := make([]*scrapper.SqlDefinitionRow, len(names))
	for i, name := range names {
		tables[i] = &scrapper.SqlDefinitionRow{
			Instance: e.conf.Host, Database: e.conf.Database, Schema: name.Schema, Table: name.Table, IsTable: true, TableType: "TABLE",
		}
	}
	return scrapper.ReadTableDdls(ctx, tables, showTableParallelism, e.showTable, isRelationGone)
}

func (e *RedshiftScrapper) showTable(ctx context.Context, table *scrapper.SqlDefinitionRow) (string, error) {
	statement, err := showTableStatement(table.Schema, table.Table)
	if err != nil {
		return "", err
	}
	var lines []string
	if err := e.executor.Select(ctx, &lines, statement); err != nil {
		return "", err
	}
	return strings.Join(lines, "\n"), nil
}

// showTableStatement quotes each part of the name, as the names come from
// pg_class and are exact.
func showTableStatement(schema, table string) (string, error) {
	name, err := sqldialect.QualifiedIdent(sqldialect.CanonicalIdent(schema), sqldialect.CanonicalIdent(table)).
		ToSql(sqldialect.NewRedshiftDialect())
	if err != nil {
		return "", err
	}
	return "SHOW TABLE " + name, nil
}

// isRelationGone reports a SHOW TABLE of a table dropped since it was listed:
// Redshift answers "Could not find relation <oid>" when the name still
// resolved, and 42P01 (undefined_table) when it no longer did.
func isRelationGone(err error) bool {
	var pqErr *pq.Error
	if errors.As(err, &pqErr) && pqErr.Code == "42P01" {
		return true
	}
	return strings.Contains(err.Error(), "Could not find relation")
}
