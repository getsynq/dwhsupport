package postgres

import (
	"context"

	dwhexecpostgres "github.com/getsynq/dwhsupport/exec/postgres"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/sqldialect"
	"github.com/lib/pq"
	_ "github.com/lib/pq"
	"github.com/pkg/errors"
)

type PostgresScapperConf = dwhexecpostgres.PostgresConf

var _ scrapper.Scrapper = &PostgresScrapper{}

type PostgresScrapper struct {
	conf     *PostgresScapperConf
	executor *dwhexecpostgres.PostgresExecutor
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

func (e *PostgresScrapper) IsPermissionError(err error) bool {
	pqError := &pq.Error{}
	if errors.As(err, &pqError) {
		return pqError.Code == "42501"
	}

	return false
}

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
