package bigquery

import (
	"context"

	"github.com/pkg/errors"

	"cloud.google.com/go/bigquery"
	"github.com/getsynq/dwhsupport/exec"
	_ "github.com/lib/pq"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

type BigQueryConf struct {
	ProjectId       string
	Region          string
	CredentialsJson string
	CredentialsFile string
}

type Executor interface {
	QueryRowsIterator(ctx context.Context, q string, args ...interface{}) (*bigquery.RowIterator, error)
}

type BigQueryExecutor struct {
	conf   *BigQueryConf
	client *bigquery.Client
}

func NewBigqueryExecutor(ctx context.Context, conf *BigQueryConf) (*BigQueryExecutor, error) {

	var options []option.ClientOption
	options = append(options, option.WithUserAgent("synq-bq-client-v1.0.0"))
	if len(conf.CredentialsJson) > 0 {
		options = append(options, option.WithCredentialsJSON([]byte(conf.CredentialsJson)))
	}
	if len(conf.CredentialsFile) > 0 {
		options = append(options, option.WithCredentialsFile(conf.CredentialsFile))
	}

	client, err := bigquery.NewClient(
		ctx,
		conf.ProjectId,
		options...,
	)
	if err != nil {
		return nil, err
	}

	// Poor man ping
	di := client.Datasets(ctx)
	_, err = di.Next()
	if errors.Is(err, iterator.Done) {
		err = nil
	}
	if err != nil {
		return nil, exec.NewAuthError(err)
	}

	return &BigQueryExecutor{client: client, conf: conf}, nil
}

func (e *BigQueryExecutor) Exec(ctx context.Context, sql string) error {
	query := e.client.Query(sql)
	job, err := query.Run(ctx)
	if err != nil {
		return err
	}

	_, err = job.Read(ctx)
	if err != nil {
		return err
	}

	return nil
}

func (e *BigQueryExecutor) QueryRowsIterator(
	ctx context.Context,
	sql string,
	args ...interface{},
) (*bigquery.RowIterator, error) {
	query := e.client.Query(sql)

	job, err := query.Run(ctx)
	if err != nil {
		return nil, err
	}

	it, err := job.Read(ctx)
	if err != nil {
		return nil, err
	}

	return it, nil
}

func (e *BigQueryExecutor) GetBigQueryClient() *bigquery.Client {
	return e.client
}

func (e *BigQueryExecutor) Close() error {
	return e.client.Close()
}
