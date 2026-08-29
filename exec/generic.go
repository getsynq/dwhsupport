package exec

import (
	"strings"

	"github.com/pkg/errors"
	"golang.org/x/xerrors"
)

// QueryMapResult is one row of a QueryMaps read, keyed by the column names the
// driver reported.
//
// The keys stay exactly as the warehouse spelled them: a caller ranging over a
// row is showing a customer their own columns back, and folding them would
// rename every column of every Snowflake query. Reading one column out by the
// alias the query asked for goes through Get instead.
type QueryMapResult map[string]interface{}

// Get reads one column, matching the name case-insensitively.
//
// Indexing the map directly is what makes a folded-case result set silent. An
// account with QUOTED_IDENTIFIERS_IGNORE_CASE = TRUE stores quoted identifiers
// upper-cased, and every alias our SQL asks for is quoted, so a row of
// `AS "fail_0"` comes back keyed FAIL_0. A caller indexing it with "fail_0"
// misses, reads the miss as zero, and reports a SQL test green that should have
// caught bad data. Nothing raises an error anywhere along that path, which is
// why the lookup has to fold rather than the caller having to know.
//
// An absent column is still absent, so an alias we got wrong stays a miss rather
// than being papered over, and a column present but NULL is found with a nil
// value. Where a result set carries two columns that differ only in case, the
// exact spelling wins.
func (r QueryMapResult) Get(column string) (interface{}, bool) {
	if value, found := r[column]; found {
		return value, true
	}
	for key, value := range r {
		if strings.EqualFold(key, column) {
			return value, true
		}
	}
	return nil, false
}

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
