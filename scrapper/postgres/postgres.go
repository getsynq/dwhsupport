package postgres

import (
	"context"

	dwhexecpostgres "github.com/getsynq/dwhsupport/exec/postgres"
	"github.com/getsynq/dwhsupport/scrapper"
	_ "github.com/lib/pq"
)

type PostgresScapperConf = dwhexecpostgres.PostgresConf

var _ scrapper.Scrapper = &PostgresScrapper{}

type PostgresScrapper struct {
	conf     *PostgresScapperConf
	executor *dwhexecpostgres.PostgresExecutor
}

func (e *PostgresScrapper) Dialect() string {
	return "postgres"
}

func NewPostgresScrapper(ctx context.Context, conf *PostgresScapperConf) (*PostgresScrapper, error) {
	executor, err := dwhexecpostgres.NewPostgresExecutor(ctx, conf)
	if err != nil {
		return nil, err
	}

	return &PostgresScrapper{
		conf:     conf,
		executor: executor,
	}, nil
}

func (e *PostgresScrapper) ValidateConfiguration(ctx context.Context) ([]string, error) {
	return nil, nil
}

func (e *PostgresScrapper) Close() error {
	return e.executor.Close()
}
