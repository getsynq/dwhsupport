package redshift

import (
	"context"
	_ "embed"

	dwhexec "github.com/getsynq/dwhsupport/exec"
	"github.com/getsynq/dwhsupport/exec/stdsql"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scope"
)

//go:embed query_table_constraints.sql
var queryTableConstraintsSql string

var querySortDistKeysSql = `
SELECT
	n.nspname AS schema_name,
	c.relname AS table_name,
	a.attname AS column_name,
	a.attsortkeyord AS sort_key_ord,
	a.attisdistkey AS is_dist_key
FROM pg_catalog.pg_attribute a
JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE (a.attsortkeyord > 0 OR a.attisdistkey = true)
	AND a.attnum > 0
	AND n.nspname NOT IN ({{system_schemas}})
	-- a materialized view keeps its rows in mv_tbl__<view>__<n> in its own schema, owned by rdsdb (usesysid 1)
	AND NOT (c.relowner = 1 AND left(c.relname, 8) = 'mv_tbl__')
	/* SYNQ_SCOPE_FILTER */
ORDER BY n.nspname, c.relname, a.attsortkeyord
`

func (e *RedshiftScrapper) QueryTableConstraints(ctx context.Context) ([]*scrapper.TableConstraintRow, error) {
	var results []*scrapper.TableConstraintRow

	// Query primary keys, unique constraints and foreign keys
	sql := scope.AppendScopeConditions(ctx, withSystemSchemas(queryTableConstraintsSql), "", "n.nspname", "c.relname")
	pkRows, err := stdsql.QueryMany[scrapper.TableConstraintRow](ctx, e.executor.GetDb(), sql,
		dwhexec.WithPostProcessors(func(row *scrapper.TableConstraintRow) (*scrapper.TableConstraintRow, error) {
			row.Database = e.conf.Database
			row.Instance = e.conf.Host
			// A foreign key cannot reference another database.
			if row.ReferencedTable != "" {
				row.ReferencedDatabase = e.conf.Database
			}
			return row, nil
		}),
	)
	if err != nil {
		return nil, err
	}
	results = append(results, pkRows...)

	// Query sort keys and dist keys
	sortDistRows, err := e.querySortDistKeys(ctx)
	if err != nil {
		return nil, err
	}
	results = append(results, sortDistRows...)

	return results, nil
}

type sortDistRow struct {
	SchemaName string `db:"schema_name"`
	TableName  string `db:"table_name"`
	ColumnName string `db:"column_name"`
	SortKeyOrd int32  `db:"sort_key_ord"`
	IsDistKey  bool   `db:"is_dist_key"`
}

func (e *RedshiftScrapper) querySortDistKeys(ctx context.Context) ([]*scrapper.TableConstraintRow, error) {
	sql := scope.AppendScopeConditions(ctx, withSystemSchemas(querySortDistKeysSql), "", "n.nspname", "c.relname")
	rows, err := stdsql.QueryMany[sortDistRow](ctx, e.executor.GetDb(), sql)
	if err != nil {
		return nil, err
	}

	var results []*scrapper.TableConstraintRow
	for _, row := range rows {
		if row.SortKeyOrd > 0 {
			results = append(results, &scrapper.TableConstraintRow{
				Instance:       e.conf.Host,
				Database:       e.conf.Database,
				Schema:         row.SchemaName,
				Table:          row.TableName,
				ConstraintName: "sortkey",
				ColumnName:     row.ColumnName,
				ConstraintType: scrapper.ConstraintTypeSortingKey,
				ColumnPosition: row.SortKeyOrd,
			})
		}
		if row.IsDistKey {
			results = append(results, &scrapper.TableConstraintRow{
				Instance:       e.conf.Host,
				Database:       e.conf.Database,
				Schema:         row.SchemaName,
				Table:          row.TableName,
				ConstraintName: "distkey",
				ColumnName:     row.ColumnName,
				ConstraintType: scrapper.ConstraintTypeDistributionKey,
				ColumnPosition: 1,
			})
		}
	}

	return results, nil
}
