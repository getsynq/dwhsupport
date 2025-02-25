package mysql

import (
	"context"

	dwhexecmysql "github.com/getsynq/dwhsupport/exec/mysql"
	"github.com/getsynq/dwhsupport/scrapper"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

type MySQLScrapperConf = dwhexecmysql.MySQLConf

type Executor interface {
	queryRows(ctx context.Context, q string, args ...interface{}) (*sqlx.Rows, error)
}

var _ scrapper.Scrapper = &MySQLScrapper{}

type MySQLScrapper struct {
	conf     *MySQLScrapperConf
	executor *dwhexecmysql.MySQLExecutor
}

func (e *MySQLScrapper) Dialect() string {
	return "mysql"
}

func NewMySQLScrapper(ctx context.Context, conf *MySQLScrapperConf) (*MySQLScrapper, error) {
	executor, err := dwhexecmysql.NewMySQLExecutor(ctx, conf)
	if err != nil {
		return nil, err
	}

	return &MySQLScrapper{conf: conf, executor: executor}, nil
}

func (e *MySQLScrapper) ValidateConfiguration(ctx context.Context) ([]string, error) {
	return nil, nil
}

func (e *MySQLScrapper) Close() error {
	return e.executor.Close()
}
