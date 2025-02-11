package exec

import (
	"github.com/pkg/errors"
	"golang.org/x/xerrors"
)

type QueryMapResult map[string]interface{}

var _ xerrors.Wrapper = &AuthError{}

type AuthError struct {
	Err error
}

func (r *AuthError) Unwrap() error {
	return r.Err
}

func (r *AuthError) Error() string {
	return errors.Wrap(r.Err, "connection error").Error()
}

func NewAuthError(err error) *AuthError {
	return &AuthError{Err: err}
}

type QueryMany[T any] struct {
	Sql              string
	Args             []interface{}
	Postprocessors   []func(row *T) (*T, error)
	ProcessBatchSize int
}

type QueryManyOpt[T any] func(*QueryMany[T])

func WithArgs[T any](args ...interface{}) QueryManyOpt[T] {
	return func(q *QueryMany[T]) {
		q.Args = args
	}
}

func WithPostProcessors[T any](fn ...func(row *T) (*T, error)) QueryManyOpt[T] {
	return func(q *QueryMany[T]) {
		q.Postprocessors = append(q.Postprocessors, fn...)
	}
}

func WithProcessBatchSize[T any](size int) QueryManyOpt[T] {
	return func(q *QueryMany[T]) {
		q.ProcessBatchSize = size
	}
}

func NewQueryMany[T any](q string) *QueryMany[T] {
	return &QueryMany[T]{Sql: q, ProcessBatchSize: 1000}
}
