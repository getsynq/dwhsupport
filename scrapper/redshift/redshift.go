package redshift

import (
	"context"

	dwhexecredshift "github.com/getsynq/dwhsupport/exec/redshift"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	_ "github.com/lib/pq"
	"github.com/pkg/errors"
)

type RedshiftScrapperConf struct {
	dwhexecredshift.RedshiftConf
	FreshnessFromQueryLogs bool
}

type Executor interface {
	queryRows(ctx context.Context, q string, args ...interface{}) (*sqlx.Rows, error)
}

var _ scrapper.Scrapper = &RedshiftScrapper{}

type RedshiftScrapper struct {
	conf     *RedshiftScrapperConf
	executor *dwhexecredshift.RedshiftExecutor
}

func NewRedshiftScrapper(ctx context.Context, conf *RedshiftScrapperConf) (*RedshiftScrapper, error) {
	executor, err := dwhexecredshift.NewRedshiftExecutor(ctx, &conf.RedshiftConf)
	if err != nil {
		return nil, err
	}

	return &RedshiftScrapper{
		conf:     conf,
		executor: executor,
	}, nil
}

func (e *RedshiftScrapper) IsPermissionError(err error) bool {
	pqError := &pq.Error{}
	if errors.As(err, &pqError) {
		return pqError.Code == "42501"
	}

	return false
}

func (e *RedshiftScrapper) Dialect() string {
	return "redshift"
}

func (e *RedshiftScrapper) Executor() *dwhexecredshift.RedshiftExecutor {
	return e.executor
}

func (e *RedshiftScrapper) ValidateConfiguration(ctx context.Context) ([]string, error) {
	return nil, nil
}

func (e *RedshiftScrapper) Close() error {
	return e.executor.Close()
}
