package clickhouse

import (
	"context"
	"database/sql/driver"
	"io"
	"time"

	"github.com/pkg/errors"
)

// noDataBlockConnector wraps clickhouse-go's connector so that a query the
// server answers without any data block (a DDL such as ALTER, CREATE or DROP)
// comes back as an empty result rather than driver.ErrBadConn.
//
// clickhouse-go reads the end of such a stream as io.EOF and reports it as a
// broken connection, so database/sql ran the statement again on new
// connections (three times in all) and still failed with "bad connection".
type noDataBlockConnector struct {
	driver.Connector
}

// pingTimeout bounds the ping that follows an ErrBadConn: a stream that ended
// cleanly answers it in one round trip, while a connection whose read timed out
// mid-answer could otherwise hold it for the driver's whole read timeout.
const pingTimeout = 5 * time.Second

func (c noDataBlockConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.Connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	// clickhouse-go's std connection implements all of these; checking once here
	// means a driver that drops one fails loudly instead of falling back silently.
	chConn, ok := conn.(chConn)
	if !ok {
		_ = conn.Close()
		return nil, errors.Errorf("clickhouse: unexpected driver connection type %T", conn)
	}
	return &noDataBlockConn{chConn: chConn}, nil
}

// chConn is what clickhouse-go's std connection implements.
type chConn interface {
	driver.Conn
	driver.QueryerContext
	driver.ExecerContext
	driver.ConnPrepareContext
	driver.ConnBeginTx
	driver.NamedValueChecker
	driver.Pinger
	driver.SessionResetter
}

// noDataBlockConn forwards everything to the driver's connection except
// QueryContext.
type noDataBlockConn struct {
	chConn
}

func (c *noDataBlockConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	rows, err := c.chConn.QueryContext(ctx, query, args)
	if !errors.Is(err, driver.ErrBadConn) {
		return rows, err
	}
	// The driver swallows the error behind ErrBadConn, so a round trip tells the
	// cases apart: only a connection whose stream ended cleanly answers a ping
	// with a pong. A dropped one fails it, and so does one that timed out while
	// the server was still sending, since the rest of that answer comes first.
	pingCtx, cancel := context.WithTimeout(ctx, pingTimeout)
	defer cancel()
	if c.chConn.Ping(pingCtx) == nil {
		return noRows{}, nil
	}
	return nil, driver.ErrBadConn
}

// noRows is the result of a statement that returned no data block.
type noRows struct{}

func (noRows) Columns() []string         { return nil }
func (noRows) Close() error              { return nil }
func (noRows) Next([]driver.Value) error { return io.EOF }
