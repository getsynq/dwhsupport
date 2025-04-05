package stdsql

import (
	"context"
	"errors"
	"fmt"

	"github.com/getsynq/dwhsupport/exec"
	"github.com/getsynq/dwhsupport/exec/querier"
	"github.com/getsynq/dwhsupport/sshtunnel"
	"github.com/jmoiron/sqlx"
)

var _ querier.Querier[interface{}] = (*StdSqlQuerier[interface{}])(nil)

type Querier_Opts[T any] func(querier *StdSqlQuerier[T])

func Querier_WithSshTunnelDialer[T any](sshTunnelDialer *sshtunnel.SshTunnelDialer) Querier_Opts[T] {
	return func(querier *StdSqlQuerier[T]) {
		querier.sshTunnelDialer = sshTunnelDialer
	}
}

func NewQuerier[T any](conn *sqlx.DB, opts ...Querier_Opts[T]) *StdSqlQuerier[T] {
	querier := &StdSqlQuerier[T]{conn: conn}

	for _, opt := range opts {
		opt(querier)
	}

	return querier
}

type StdSqlQuerier[T any] struct {
	conn            *sqlx.DB
	sshTunnelDialer *sshtunnel.SshTunnelDialer
}

func (s StdSqlQuerier[T]) QueryMany(ctx context.Context, sql string, opts ...exec.QueryManyOpt[T]) ([]*T, error) {
	return QueryMany(ctx, s.conn, sql, opts...)
}

func (s StdSqlQuerier[T]) QueryAndProcessMany(
	ctx context.Context,
	sql string,
	handler func(ctx context.Context, batch []*T) error,
	opts ...exec.QueryManyOpt[T],
) error {
	return QueryAndProcessMany(ctx, s.conn, sql, handler, opts...)
}

func (s StdSqlQuerier[T]) QueryMaps(ctx context.Context, sql string) ([]exec.QueryMapResult, error) {
	return QueryMaps(ctx, s.conn, sql)
}

func (s StdSqlQuerier[T]) Exec(ctx context.Context, sql string) error {
	return Exec(ctx, s.conn, sql)
}

func (s StdSqlQuerier[T]) Close() error {
	var errs []error
	if err := s.conn.Close(); err != nil {
		errs = append(errs, err)
	}

	if s.sshTunnelDialer != nil {
		fmt.Println("closing ssh tunnel")
		if err := s.sshTunnelDialer.Close(); err != nil {
			fmt.Println("error closing ssh tunnel", err)
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}
