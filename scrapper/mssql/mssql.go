package mssql

import (
	"context"
	"strings"

	dwhexecmssql "github.com/getsynq/dwhsupport/exec/mssql"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/sqldialect"
)

type MSSQLScrapperConf = dwhexecmssql.MSSQLConf

var _ scrapper.Scrapper = &MSSQLScrapper{}

type MSSQLScrapper struct {
	conf     *MSSQLScrapperConf
	executor *dwhexecmssql.MSSQLExecutor
}

func NewMSSQLScrapper(ctx context.Context, conf *MSSQLScrapperConf) (*MSSQLScrapper, error) {
	executor, err := dwhexecmssql.NewMSSQLExecutor(ctx, conf)
	if err != nil {
		return nil, err
	}

	return &MSSQLScrapper{
		conf:     conf,
		executor: executor,
	}, nil
}

func (e *MSSQLScrapper) IsPermissionError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	// Msg 229: The SELECT permission was denied
	// Msg 230: The SELECT permission was denied on column
	// Msg 262: CREATE DATABASE permission denied
	return strings.Contains(errStr, "permission was denied") ||
		strings.Contains(errStr, "permission denied")
}

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
