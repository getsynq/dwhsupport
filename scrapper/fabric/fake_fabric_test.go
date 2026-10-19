package fabric

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"io"
	"strings"
	"sync"
	"testing"

	dwhexecfabric "github.com/getsynq/dwhsupport/exec/fabric"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

// fakeRoute answers every statement match accepts, with rows or with err.
type fakeRoute struct {
	match   func(sql string) bool
	columns []string
	rows    [][]driver.Value
	err     error
}

// sqlContains matches a statement holding every one of parts.
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

// fakeFabric is a database/sql connector that answers statements from routes,
// the first matching route winning, and fails a statement no route matches.
// The scrapper reads the databases concurrently, so it is safe for that.
type fakeFabric struct {
	t      testing.TB
	routes []fakeRoute

	mu   sync.Mutex
	seen []string
}

func (f *fakeFabric) Connect(context.Context) (driver.Conn, error) { return &fakeConn{f: f}, nil }
func (f *fakeFabric) Driver() driver.Driver                        { return fakeDriver{} }

func (f *fakeFabric) query(ctx context.Context, sql string) (driver.Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	f.seen = append(f.seen, sql)
	f.mu.Unlock()
	for _, route := range f.routes {
		if !route.match(sql) {
			continue
		}
		if route.err != nil {
			return nil, route.err
		}
		return &fakeRows{columns: route.columns, rows: route.rows}, nil
	}
	f.t.Errorf("fake Fabric has no route for:\n%s", sql)
	return nil, errors.New("fake Fabric: no route")
}

type fakeDriver struct{}

func (fakeDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("open through the connector")
}

type fakeConn struct{ f *fakeFabric }

func (c *fakeConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("not supported") }
func (c *fakeConn) Close() error                        { return nil }
func (c *fakeConn) Begin() (driver.Tx, error)           { return nil, errors.New("not supported") }

func (c *fakeConn) QueryContext(ctx context.Context, sql string, _ []driver.NamedValue) (driver.Rows, error) {
	return c.f.query(ctx, sql)
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

// newFakeFabricScrapper builds a scrapper reading from routes, as
// NewFabricScrapper builds one reading from a workspace.
func newFakeFabricScrapper(t testing.TB, routes ...[]fakeRoute) (*FabricScrapper, *fakeFabric) {
	fake := &fakeFabric{t: t}
	for _, r := range routes {
		fake.routes = append(fake.routes, r...)
	}
	db := sqlx.NewDb(sql.OpenDB(fake), "sqlserver")
	t.Cleanup(func() { db.Close() })
	conf := &FabricScrapperConf{FabricConf: dwhexecfabric.FabricConf{Host: "workspace.datawarehouse.fabric.microsoft.com"}}
	return newFabricScrapperWithExecutor(context.Background(), conf, dwhexecfabric.NewFabricExecutorFromDb(&conf.FabricConf, db)), fake
}
