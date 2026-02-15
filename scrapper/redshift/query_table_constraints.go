package redshift

import (
	"context"
	_ "embed"
	"fmt"

	dwhexec "github.com/getsynq/dwhsupport/exec"
	"github.com/getsynq/dwhsupport/exec/stdsql"
	"github.com/getsynq/dwhsupport/scrapper"
)

//go:embed query_table_constraints.sql
var queryTableConstraintsSql string

func (e *RedshiftScrapper) QueryTableConstraints(ctx context.Context, database string, schema string, table string) ([]*scrapper.TableConstraintRow, error) {
	var results []*scrapper.TableConstraintRow

	// Query primary keys and unique constraints
	pkRows, err := stdsql.QueryMany[scrapper.TableConstraintRow](ctx, e.executor.GetDb(), queryTableConstraintsSql,
		dwhexec.WithArgs[scrapper.TableConstraintRow](schema, table),
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

	// Query sort keys and dist keys from SVV_TABLE_INFO
	sortDistRows, err := e.querySortDistKeys(ctx, schema, table)
	if err != nil {
		return nil, err
	}
	results = append(results, sortDistRows...)

	return results, nil
}

func (e *RedshiftScrapper) querySortDistKeys(ctx context.Context, schema string, table string) ([]*scrapper.TableConstraintRow, error) {
	query := fmt.Sprintf(`
		SELECT
			a.attname AS column_name,
			a.attsortkeyord AS sort_key_ord,
			a.attisdistkey AS is_dist_key
		FROM pg_catalog.pg_attribute a
		JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
		JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = '%s'
			AND c.relname = '%s'
			AND (a.attsortkeyord > 0 OR a.attisdistkey = true)
			AND a.attnum > 0
		ORDER BY a.attsortkeyord
	`, schema, table)

	type sortDistRow struct {
		ColumnName string `db:"column_name"`
		SortKeyOrd int32  `db:"sort_key_ord"`
		IsDistKey  bool   `db:"is_dist_key"`
	}

	rows, err := stdsql.QueryMany[sortDistRow](ctx, e.executor.GetDb(), query)
	if err != nil {
		return nil, err
	}

	var results []*scrapper.TableConstraintRow
	for _, row := range rows {
		if row.SortKeyOrd > 0 {
			results = append(results, &scrapper.TableConstraintRow{
				Instance:       e.conf.Host,
				Database:       e.conf.Database,
				Schema:         schema,
				Table:          table,
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
				Schema:         schema,
				Table:          table,
				ConstraintName: "distkey",
				ColumnName:     row.ColumnName,
				ConstraintType: scrapper.ConstraintTypeDistributionKey,
				ColumnPosition: 1,
			})
		}
	}

	return results, nil
}
