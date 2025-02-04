package clickhouse

import (
	"context"
	_ "embed"
	"fmt"

	dwhexec "github.com/getsynq/dwhsupport/exec"
	dwhexecclickhouse "github.com/getsynq/dwhsupport/exec/clickhouse"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/xxjwxc/gowp/workpool"
)

//go:embed query_sql_definitions.sql
var querySqlDefinitionsSql string

func (e *ClickhouseScrapper) QuerySqlDefinitions(ctx context.Context) ([]*scrapper.SqlDefinitionRow, error) {

	sqlDefs, err := dwhexecclickhouse.NewQuerier[scrapper.SqlDefinitionRow](e.executor).QueryMany(ctx, querySqlDefinitionsSql,
		dwhexec.WithPostProcessors[scrapper.SqlDefinitionRow](func(row *scrapper.SqlDefinitionRow) (*scrapper.SqlDefinitionRow, error) {
			row.Database = e.conf.DatabaseName
			return row, nil
		}),
	)

	if err != nil {
		return nil, err
	}

	pool := workpool.New(4)

	for _, sqlDef := range sqlDefs {
		sqlDef := sqlDef
		pool.Do(func() error {
			if sqlDef.IsView {
				if sql, err := e.showCreateView(ctx, sqlDef.Schema, sqlDef.Table); err == nil {
					sqlDef.Sql = sql
				}
			} else {
				if sql, err := e.showCreateTable(ctx, sqlDef.Schema, sqlDef.Table); err == nil {
					sqlDef.Sql = sql
				}
			}
			return nil
		})
	}

	err = pool.Wait()
	if err != nil {
		return nil, err
	}
	return sqlDefs, nil
}

func (e *ClickhouseScrapper) showCreateView(ctx context.Context, schema string, table string) (string, error) {
	sql := fmt.Sprintf("SHOW CREATE VIEW `%s`.`%s`", schema, table)
	outSql := ""
	if err := e.executor.QueryRow(ctx, sql).Scan(&outSql); err != nil {
		return "", err
	}
	return outSql, nil
}

func (e *ClickhouseScrapper) showCreateTable(ctx context.Context, schema string, table string) (string, error) {
	sql := fmt.Sprintf("SHOW CREATE TABLE `%s`.`%s`", schema, table)
	outSql := ""
	if err := e.executor.QueryRow(ctx, sql).Scan(&outSql); err != nil {
		return "", err
	}
	return outSql, nil
}
