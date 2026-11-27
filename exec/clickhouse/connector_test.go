package clickhouse

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeChConn stands in for clickhouse-go's database/sql connection: every query
// fails with queryErr, and a ping answers pingErr.
type fakeChConn struct {
	chConn
	queryErr error
	pingErr  error
	queries  int
	pings    int
}

func (c *fakeChConn) Close() error { return nil }

func (c *fakeChConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	c.queries++
	return nil, c.queryErr
}

func (c *fakeChConn) Ping(context.Context) error {
	c.pings++
	return c.pingErr
}

// fakeChConnector hands out connections whose queries all end without a data
// block (driver.ErrBadConn) and whose pings answer pingErr.
type fakeChConnector struct {
	pingErr error
	conns   []*fakeChConn
}

func (c *fakeChConnector) Connect(context.Context) (driver.Conn, error) {
	conn := &fakeChConn{queryErr: driver.ErrBadConn, pingErr: c.pingErr}
	c.conns = append(c.conns, conn)
	return conn, nil
}

func (c *fakeChConnector) Driver() driver.Driver { return nil }

func TestQueryOnABrokenConnectionStillReportsBadConn(t *testing.T) {
	conn := &fakeChConn{queryErr: driver.ErrBadConn, pingErr: errors.New("unexpected packet 1")}
	_, err := (&noDataBlockConn{chConn: conn}).QueryContext(context.Background(), "SELECT 1", nil)
	assert.ErrorIs(t, err, driver.ErrBadConn)
	assert.Equal(t, 1, conn.queries)
}

func TestOtherQueryErrorsPassThroughWithoutAPing(t *testing.T) {
	conn := &fakeChConn{queryErr: errors.New("code: 60, message: Table default.t does not exist")}
	_, err := (&noDataBlockConn{chConn: conn}).QueryContext(context.Background(), "SELECT 1", nil)
	assert.EqualError(t, err, "code: 60, message: Table default.t does not exist")
	assert.Equal(t, 0, conn.pings)
}

// clickhouse-go's std connection must keep every interface chConn lists: Connect
// refuses a connection that lacks one, which would break every connection after
// a driver bump. Its driver is the connection, so no server is needed.
func TestClickhouseGoConnectionSatisfiesChConn(t *testing.T) {
	db, err := sql.Open("clickhouse", "clickhouse://127.0.0.1:1/default")
	require.NoError(t, err)
	defer db.Close()
	assert.Implements(t, (*chConn)(nil), db.Driver())
}

// Through database/sql: a broken connection is still retried on a new one, and
// a statement that ran on a healthy one is not run again.
func TestDatabaseSqlRetriesOnlyABrokenConnection(t *testing.T) {
	broken := &fakeChConnector{pingErr: driver.ErrBadConn}
	_, err := sql.OpenDB(noDataBlockConnector{broken}).QueryContext(context.Background(), "SELECT 1")
	assert.ErrorIs(t, err, driver.ErrBadConn)
	assert.Greater(t, len(broken.conns), 1, "database/sql should have tried another connection")

	healthy := &fakeChConnector{}
	rows, err := sql.OpenDB(noDataBlockConnector{healthy}).QueryContext(context.Background(), "ALTER TABLE t MODIFY COMMENT 'x'")
	require.NoError(t, err)
	assert.False(t, rows.Next())
	require.NoError(t, rows.Close())
	require.Len(t, healthy.conns, 1)
	assert.Equal(t, 1, healthy.conns[0].queries)
}
