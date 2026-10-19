package snowflake

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"io"
	"strings"
	"sync"
	"testing"

	dwhexecsnowflake "github.com/getsynq/dwhsupport/exec/snowflake"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

// fakeSnowflake is a database/sql driver answering from canned results, so a
// test can put a scrapper in front of an account failing in a chosen way
// without a warehouse. Each statement is offered to the routes in order, and
// the first that matches answers it, with rows or with the error the real
// driver would return.
type fakeSnowflake struct {
	t      *testing.T
	routes []fakeRoute

	mu   sync.Mutex
	seen []string
}

type fakeRoute struct {
	match   func(sql string) bool
	columns []string
	rows    [][]driver.Value
	err     error
}

func sqlContains(parts ...string) func(string) bool {
	return func(sql string) bool {
		for _, part := range parts {
			if !strings.Contains(sql, part) {
				return false
			}
		}
		return true
	}
}

// statements returns every statement the scrapper sent that matches.
func (f *fakeSnowflake) statements(match func(string) bool) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for _, s := range f.seen {
		if match(s) {
			out = append(out, s)
		}
	}
	return out
}

func (f *fakeSnowflake) query(ctx context.Context, query string) (driver.Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	f.seen = append(f.seen, query)
	f.mu.Unlock()
	for _, route := range f.routes {
		if !route.match(query) {
			continue
		}
		if route.err != nil {
			return nil, route.err
		}
		return &fakeRows{columns: route.columns, rows: route.rows}, nil
	}
	f.t.Errorf("fake Snowflake has no answer for: %s", query)
	return nil, errors.Errorf("fake Snowflake has no answer for: %s", query)
}

func (f *fakeSnowflake) Connect(context.Context) (driver.Conn, error) { return &fakeConn{f: f}, nil }
func (f *fakeSnowflake) Driver() driver.Driver                        { return fakeDriver{} }

type fakeDriver struct{}

func (fakeDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("open the fake through its connector")
}

type fakeConn struct{ f *fakeSnowflake }

func (c *fakeConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if len(args) > 0 {
		return nil, errors.New("fake Snowflake does not bind arguments")
	}
	return c.f.query(ctx, query)
}

func (c *fakeConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("fake Snowflake answers queries only")
}
func (c *fakeConn) Close() error { return nil }
func (c *fakeConn) Begin() (driver.Tx, error) {
	return nil, errors.New("fake Snowflake has no transactions")
}

type fakeRows struct {
	columns []string
	rows    [][]driver.Value
	next    int
}

func (r *fakeRows) Columns() []string { return r.columns }
func (r *fakeRows) Close() error      { return nil }
func (r *fakeRows) Next(dest []driver.Value) error {
	if r.next >= len(r.rows) {
		return io.EOF
	}
	copy(dest, r.rows[r.next])
	r.next++
	return nil
}

// newFakeSnowflakeScrapper builds the scrapper the product builds, on the fake.
func newFakeSnowflakeScrapper(ctx context.Context, fake *fakeSnowflake, conf *SnowflakeScrapperConf) *SnowflakeScrapper {
	db := sqlx.NewDb(sql.OpenDB(fake), "snowflake")
	fake.t.Cleanup(func() { _ = db.Close() })
	return newSnowflakeScrapper(ctx, conf, dwhexecsnowflake.NewSnowflakeExecutorWithDb(&conf.SnowflakeConf, db))
}
