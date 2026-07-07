package postgres

import (
	"context"

	dwhexecpostgres "github.com/getsynq/dwhsupport/exec/postgres"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/sqldialect"
	_ "github.com/lib/pq"
)

type PostgresScapperConf struct {
	dwhexecpostgres.PostgresConf
}

var _ scrapper.Scrapper = &PostgresScrapper{}

type PostgresScrapper struct {
	conf     *PostgresScapperConf
	executor *dwhexecpostgres.PostgresExecutor
}

func NewPostgresScrapper(ctx context.Context, conf *PostgresScapperConf) (*PostgresScrapper, error) {
	executor, err := dwhexecpostgres.NewPostgresExecutor(ctx, &conf.PostgresConf)
	if err != nil {
		return nil, err
	}

	return &PostgresScrapper{
		conf:     conf,
		executor: executor,
	}, nil
}

func (e *PostgresScrapper) Executor() *dwhexecpostgres.PostgresExecutor {
	return e.executor
}

func (e *PostgresScrapper) IsPermissionError(err error) bool {
	return dwhexecpostgres.IsPermissionError(err)
}

func (e *PostgresScrapper) Capabilities() scrapper.Capabilities { return scrapper.Capabilities{} }

func (e *PostgresScrapper) DialectType() string {
	return "postgres"
}

func (e *PostgresScrapper) SqlDialect() sqldialect.Dialect {
	return sqldialect.NewPostgresDialect()
}

func (e *PostgresScrapper) ValidateConfiguration(ctx context.Context) ([]string, error) {
	return nil, nil
}

func (e *PostgresScrapper) Close() error {
	return e.executor.Close()
}
