package mysql

import (
	"context"
	_ "embed"
	"fmt"
	"regexp"

	dwhexec "github.com/getsynq/dwhsupport/exec"
	dwhexecmysql "github.com/getsynq/dwhsupport/exec/mysql"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/xxjwxc/gowp/workpool"
)

//go:embed query_sql_definitions.sql
var querySqlDefinitionsSql string

func (e *MySQLScrapper) QuerySqlDefinitions(ctx context.Context) ([]*scrapper.SqlDefinitionRow, error) {

	sqlDefs, err := dwhexecmysql.NewQuerier[scrapper.SqlDefinitionRow](e.executor).QueryMany(ctx, querySqlDefinitionsSql,
		dwhexec.WithPostProcessors(func(row *scrapper.SqlDefinitionRow) (*scrapper.SqlDefinitionRow, error) {
			row.Database = e.conf.Host
			return row, nil
		}))
	if err != nil {
		return nil, err
	}

	pool := workpool.New(4)

	for _, sqlDef := range sqlDefs {
		sqlDef := sqlDef
		if len(sqlDef.Sql) > 0 {
			continue
		}
		pool.Do(func() error {
			if sqlDef.IsView {
				if sql, err := e.showCreateView(ctx, sqlDef.Schema, sqlDef.Table); err == nil && len(sql) > 0 {
					sqlDef.Sql = sql
				}
			} else {
				if sql, err := e.showCreateTable(ctx, sqlDef.Schema, sqlDef.Table); err == nil && len(sql) > 0 {
					sqlDef.Sql = removeDynamicPartsOfSql(sql)
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

var removeAutoIncrementValueRegexp = regexp.MustCompile(`AUTO_INCREMENT=\d+\W*`)

func removeDynamicPartsOfSql(sql string) string {
	return removeAutoIncrementValueRegexp.ReplaceAllString(sql, "")
}

func (e *MySQLScrapper) showCreateView(ctx context.Context, schema string, table string) (string, error) {
	sql := fmt.Sprintf("SHOW CREATE VIEW `%s`.`%s`", schema, table)
	var outSql []struct {
		View                string `db:"View"`
		CreateView          string `db:"Create View"`
		CharacterSetClient  string `db:"character_set_client"`
		CollationConnection string `db:"collation_connection"`
	}
	if err := e.executor.GetDb().SelectContext(ctx, &outSql, sql); err != nil {
		return "", err
	}
	if len(outSql) == 0 {
		return "", nil
	}

	return outSql[0].CreateView, nil
}

func (e *MySQLScrapper) showCreateTable(ctx context.Context, schema string, table string) (string, error) {
	sql := fmt.Sprintf("SHOW CREATE TABLE `%s`.`%s`", schema, table)
	var outSql []struct {
		Table       string `db:"Table"`
		CreateTable string `db:"Create Table"`
	}
	if err := e.executor.GetDb().SelectContext(ctx, &outSql, sql); err != nil {
		return "", err
	}
	if len(outSql) == 0 {
		return "", nil
	}

	return outSql[0].CreateTable, nil
}
