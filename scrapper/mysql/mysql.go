package mysql

import (
	"context"

	dwhexecmysql "github.com/getsynq/dwhsupport/exec/mysql"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/sqldialect"
	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
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

func NewMySQLScrapper(ctx context.Context, conf *MySQLScrapperConf) (*MySQLScrapper, error) {
	executor, err := dwhexecmysql.NewMySQLExecutor(ctx, conf)
	if err != nil {
		return nil, err
	}

	return &MySQLScrapper{conf: conf, executor: executor}, nil
}

func (e *MySQLScrapper) IsPermissionError(err error) bool {
	mySqlError := &mysql.MySQLError{}
	if errors.As(err, &mySqlError) {
		return mySqlError.Number == 1044
	}

	return false
}

func (e *MySQLScrapper) DialectType() string {
	return "mysql"
}

func (e *MySQLScrapper) SqlDialect() sqldialect.Dialect {
	return sqldialect.NewMySQLDialect()
}

func (e *MySQLScrapper) ValidateConfiguration(ctx context.Context) ([]string, error) {
	return nil, nil
}

func (e *MySQLScrapper) Close() error {
	return e.executor.Close()
}
