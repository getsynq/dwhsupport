package db2

import (
	"context"

	dwhexecdb2 "github.com/getsynq/dwhsupport/exec/db2"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/sqldialect"
)

// Db2ScrapperConf configures a scrapper for IBM Db2 for Linux, UNIX and
// Windows. Db2 for z/OS and Db2 for i keep their catalogs elsewhere
// (SYSIBM.SYS*, QSYS2) and are not supported.
type Db2ScrapperConf struct {
	dwhexecdb2.Db2Conf
}

var _ scrapper.Scrapper = &Db2Scrapper{}

type Db2Scrapper struct {
	conf     *Db2ScrapperConf
	executor *dwhexecdb2.Db2Executor
}

func NewDb2Scrapper(ctx context.Context, conf *Db2ScrapperConf) (*Db2Scrapper, error) {
	executor, err := dwhexecdb2.NewDb2Executor(ctx, &conf.Db2Conf)
	if err != nil {
		return nil, err
	}

	return &Db2Scrapper{
		conf:     conf,
		executor: executor,
	}, nil
}

func (e *Db2Scrapper) Executor() *dwhexecdb2.Db2Executor {
	return e.executor
}

func (e *Db2Scrapper) IsPermissionError(err error) bool {
	return dwhexecdb2.IsPermissionError(err)
}

func (e *Db2Scrapper) Capabilities() scrapper.Capabilities { return scrapper.Capabilities{} }

func (e *Db2Scrapper) DialectType() string {
	return "db2"
}

func (e *Db2Scrapper) SqlDialect() sqldialect.Dialect {
	return sqldialect.NewDb2Dialect()
}

func (e *Db2Scrapper) ValidateConfiguration(ctx context.Context) ([]string, error) {
	return nil, nil
}

func (e *Db2Scrapper) Close() error {
	return e.executor.Close()
}
