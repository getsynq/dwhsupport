package redshift

import (
	"context"
	"database/sql/driver"
	"strings"

	"github.com/pkg/errors"
)

// unexpectedExtendedQueryMessage is how lib/pq fails a query when Redshift
// Serverless answers it out of protocol: the first query that needs compute
// after the workgroup was idle gets BindComplete twice, about ten seconds
// late, while the compute resumes. lib/pq marks the connection broken but
// returns its own error rather than driver.ErrBadConn, so database/sql never
// tries another connection, and the next call works. lib/pq has no error type
// for it, so the text is the only way to tell.
const unexpectedExtendedQueryMessage = "pq: unexpected message during extended query execution"

// retryReadsConnector wraps lib/pq's connector so that a read failed by
// Redshift's out-of-protocol answer is reported as driver.ErrBadConn, which
// makes database/sql run it again on a fresh connection. Only QueryContext is
// translated: an Exec may write, and running it twice could repeat the write.
type retryReadsConnector struct {
	driver.Connector
}

func (c retryReadsConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.Connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &retryReadsConn{conn: conn}, nil
}

// brokenReadError keeps lib/pq's message for the caller and says
// driver.ErrBadConn to database/sql.
type brokenReadError struct {
	err error
}

func (e *brokenReadError) Error() string   { return e.err.Error() }
func (e *brokenReadError) Unwrap() []error { return []error{driver.ErrBadConn, e.err} }

func isUnexpectedExtendedQueryMessage(err error) bool {
	return err != nil && strings.HasPrefix(err.Error(), unexpectedExtendedQueryMessage)
}

// retryReadsConn forwards every interface lib/pq's connection implements.
type retryReadsConn struct {
	conn driver.Conn
}

var (
	_ driver.Conn               = (*retryReadsConn)(nil)
	_ driver.QueryerContext     = (*retryReadsConn)(nil)
	_ driver.ExecerContext      = (*retryReadsConn)(nil)
	_ driver.ConnPrepareContext = (*retryReadsConn)(nil)
	_ driver.ConnBeginTx        = (*retryReadsConn)(nil)
	_ driver.Pinger             = (*retryReadsConn)(nil)
	_ driver.SessionResetter    = (*retryReadsConn)(nil)
	_ driver.Validator          = (*retryReadsConn)(nil)
)

func (c *retryReadsConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	queryer, ok := c.conn.(driver.QueryerContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	rows, err := queryer.QueryContext(ctx, query, args)
	if isUnexpectedExtendedQueryMessage(err) {
		return nil, &brokenReadError{err: err}
	}
	return rows, err
}

func (c *retryReadsConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	execer, ok := c.conn.(driver.ExecerContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	return execer.ExecContext(ctx, query, args)
}

func (c *retryReadsConn) Prepare(query string) (driver.Stmt, error) {
	return c.conn.Prepare(query)
}

func (c *retryReadsConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if preparer, ok := c.conn.(driver.ConnPrepareContext); ok {
		return preparer.PrepareContext(ctx, query)
	}
	return c.conn.Prepare(query)
}

func (c *retryReadsConn) Close() error {
	return c.conn.Close()
}

func (c *retryReadsConn) Begin() (driver.Tx, error) {
	//nolint:staticcheck // forwarding the deprecated method is what a wrapper must do
	return c.conn.Begin()
}

func (c *retryReadsConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if beginner, ok := c.conn.(driver.ConnBeginTx); ok {
		return beginner.BeginTx(ctx, opts)
	}
	return nil, errors.New("redshift: the driver connection cannot begin a transaction with options")
}

func (c *retryReadsConn) Ping(ctx context.Context) error {
	if pinger, ok := c.conn.(driver.Pinger); ok {
		return pinger.Ping(ctx)
	}
	return nil
}

func (c *retryReadsConn) ResetSession(ctx context.Context) error {
	if resetter, ok := c.conn.(driver.SessionResetter); ok {
		return resetter.ResetSession(ctx)
	}
	return nil
}

func (c *retryReadsConn) IsValid() bool {
	if validator, ok := c.conn.(driver.Validator); ok {
		return validator.IsValid()
	}
	return true
}
