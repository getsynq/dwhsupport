package postgres

import (
	"context"
	_ "embed"

	dwhexecpostgres "github.com/getsynq/dwhsupport/exec/postgres"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scope"
	_ "github.com/lib/pq"
	"github.com/samber/lo"
)

//go:embed query_tables.sql
var queryTablesSql string

// postgresFdw is the wrapper whose options name a remote Postgres table.
const postgresFdw = "postgres_fdw"

// tableRow is a TableRow with the foreign table columns of query_tables.sql,
// all NULL for an object that is not a foreign table.
type tableRow struct {
	scrapper.TableRow
	ForeignWrapper  *string `db:"foreign_wrapper"`
	ForeignServer   *string `db:"foreign_server"`
	ForeignHost     *string `db:"foreign_host"`
	ForeignPort     *string `db:"foreign_port"`
	ForeignDatabase *string `db:"foreign_database"`
	ForeignSchema   *string `db:"foreign_schema"`
	ForeignTable    *string `db:"foreign_table"`
}

func (e *PostgresScrapper) QueryTables(ctx context.Context, opts ...scrapper.QueryTablesOption) ([]*scrapper.TableRow, error) {
	sql := scope.AppendScopeConditions(ctx, queryTablesSql, "", "sch.nspname", "tbl.relname")
	rows, err := dwhexecpostgres.NewQuerier[tableRow](e.executor).QueryMany(ctx, sql)
	if err != nil {
		return nil, err
	}
	res := make([]*scrapper.TableRow, 0, len(rows))
	for _, row := range rows {
		out := row.TableRow
		out.Database = e.conf.Database
		out.Instance = e.conf.Host
		out.ForeignOf = row.foreignOf()
		res = append(res, &out)
	}
	return res, nil
}

// foreignOf is where a foreign table reads its rows from, nil for any other
// object. Only postgres_fdw's options are read for the remote table: the other
// wrappers name theirs in options of their own, or have no remote table.
func (r *tableRow) foreignOf() *scrapper.TableForeignOf {
	if r.ForeignWrapper == nil || r.ForeignServer == nil {
		return nil
	}
	foreignOf := &scrapper.TableForeignOf{Wrapper: *r.ForeignWrapper, Server: *r.ForeignServer}
	if foreignOf.Wrapper != postgresFdw {
		return foreignOf
	}
	foreignOf.Host = lo.FromPtr(r.ForeignHost)
	foreignOf.Port = lo.FromPtr(r.ForeignPort)
	foreignOf.Database = lo.FromPtr(r.ForeignDatabase)
	// postgres_fdw reads the remote table of the foreign table's own schema and
	// name unless an option says otherwise.
	foreignOf.Schema = r.Schema
	if r.ForeignSchema != nil {
		foreignOf.Schema = *r.ForeignSchema
	}
	foreignOf.Table = r.Table
	if r.ForeignTable != nil {
		foreignOf.Table = *r.ForeignTable
	}
	return foreignOf
}
