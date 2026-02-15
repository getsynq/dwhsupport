package redshift

import (
	"context"
	_ "embed"

	dwhexec "github.com/getsynq/dwhsupport/exec"
	"github.com/getsynq/dwhsupport/exec/stdsql"
	"github.com/getsynq/dwhsupport/scrapper"
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
ORDER BY n.nspname, c.relname, a.attsortkeyord
`

func (e *RedshiftScrapper) QueryTableConstraints(ctx context.Context) ([]*scrapper.TableConstraintRow, error) {
	var results []*scrapper.TableConstraintRow

	// Query primary keys and unique constraints
	pkRows, err := stdsql.QueryMany[scrapper.TableConstraintRow](ctx, e.executor.GetDb(), queryTableConstraintsSql,
		dwhexec.WithPostProcessors(func(row *scrapper.TableConstraintRow) (*scrapper.TableConstraintRow, error) {
			row.Database = e.conf.Database
			row.Instance = e.conf.Host
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
	rows, err := stdsql.QueryMany[sortDistRow](ctx, e.executor.GetDb(), querySortDistKeysSql)
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
