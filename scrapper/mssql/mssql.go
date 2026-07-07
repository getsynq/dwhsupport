package mssql

import (
	"context"

	dwhexecmssql "github.com/getsynq/dwhsupport/exec/mssql"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/sqldialect"
)

type MSSQLScrapperConf struct {
	dwhexecmssql.MSSQLConf
}

var _ scrapper.Scrapper = &MSSQLScrapper{}

type MSSQLScrapper struct {
	conf     *MSSQLScrapperConf
	executor *dwhexecmssql.MSSQLExecutor
}

func NewMSSQLScrapper(ctx context.Context, conf *MSSQLScrapperConf) (*MSSQLScrapper, error) {
	executor, err := dwhexecmssql.NewMSSQLExecutor(ctx, &conf.MSSQLConf)
	if err != nil {
		return nil, err
	}

	return &MSSQLScrapper{
		conf:     conf,
		executor: executor,
	}, nil
}

func (e *MSSQLScrapper) Executor() *dwhexecmssql.MSSQLExecutor {
	return e.executor
}

func (e *MSSQLScrapper) IsPermissionError(err error) bool {
	return dwhexecmssql.IsPermissionError(err)
}

func (e *MSSQLScrapper) Capabilities() scrapper.Capabilities { return scrapper.Capabilities{} }

func (e *MSSQLScrapper) DialectType() string {
	return "mssql"
}

func (e *MSSQLScrapper) SqlDialect() sqldialect.Dialect {
	return sqldialect.NewMSSQLDialect()
}

func (e *MSSQLScrapper) ValidateConfiguration(ctx context.Context) ([]string, error) {
	return nil, nil
}

func (e *MSSQLScrapper) Close() error {
	return e.executor.Close()
}
