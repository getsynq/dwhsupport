package clickhouse

import (
	"context"
	_ "embed"
	"strings"

	chgo "github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	dwhexecclickhouse "github.com/getsynq/dwhsupport/exec/clickhouse"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/sqldialect"
)

type ClickhouseScrapperConf struct {
	dwhexecclickhouse.ClickhouseConf
	DatabaseName string
}

var _ scrapper.Scrapper = &ClickhouseScrapper{}

type ClickhouseScrapper struct {
	conf     ClickhouseScrapperConf
	executor *dwhexecclickhouse.ClickhouseExecutor
}

func NewClickhouseScrapper(ctx context.Context, conf ClickhouseScrapperConf) (*ClickhouseScrapper, error) {
	executor, err := dwhexecclickhouse.NewClickhouseExecutor(ctx, &conf.ClickhouseConf)
	if err != nil {
		return nil, err
	}

	return &ClickhouseScrapper{executor: executor, conf: conf}, nil
}

func (e *ClickhouseScrapper) IsPermissionError(err error) bool {
	if err == nil {
		return false
	}
	// Code 497: NOT_ENOUGH_PRIVILEGES (not defined as constant in ch-go)
	if chgo.IsErr(err, proto.Error(497)) {
		return true
	}
	return strings.Contains(err.Error(), "Not enough privileges")
}

func (e *ClickhouseScrapper) DialectType() string {
	return "clickhouse"
}

func (e *ClickhouseScrapper) SqlDialect() sqldialect.Dialect {
	return sqldialect.NewClickHouseDialect()
}

func (e *ClickhouseScrapper) Executor() *dwhexecclickhouse.ClickhouseExecutor {
	return e.executor
}

func (e *ClickhouseScrapper) ValidateConfiguration(ctx context.Context) ([]string, error) {
	return nil, nil
}

func (e *ClickhouseScrapper) Close() error {
	return e.executor.Close()
}
