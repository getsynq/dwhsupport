package clickhouse

import (
	"context"
	"crypto/tls"
	_ "embed"
	"fmt"
	"time"

	"github.com/getsynq/dwhsupport/exec"
	"github.com/getsynq/dwhsupport/exec/querycontext"
	"github.com/getsynq/dwhsupport/exec/querystats"
	"github.com/getsynq/dwhsupport/exec/stdsql"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"

	"github.com/ClickHouse/clickhouse-go/v2"
)

type ClickhouseConf struct {
	Hostname        string
	Port            int
	Username        string
	Password        string
	DefaultDatabase string
	NoSsl           bool
}

var _ stdsql.StdSqlExecutor = &ClickhouseExecutor{}

type ClickhouseExecutor struct {
	conf *ClickhouseConf
	db   *sqlx.DB
}

func (e *ClickhouseExecutor) GetDb() *sqlx.DB {
	return e.db
}

func (e *ClickhouseExecutor) Close() error {
	return e.db.Close()
}

func NewClickhouseExecutor(ctx context.Context, conf *ClickhouseConf) (*ClickhouseExecutor, error) {

	clickhouseOptions := &clickhouse.Options{
		Protocol:    clickhouse.Native,
		DialTimeout: 30 * time.Second,
		Addr:        []string{fmt.Sprintf("%s:%d", conf.Hostname, conf.Port)},
		Auth: clickhouse.Auth{
			Username: conf.Username,
			Password: conf.Password,
		},

		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},

		ConnOpenStrategy: clickhouse.ConnOpenRoundRobin,
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
			"max_query_size":     10000000,
		},
		ClientInfo: clickhouse.ClientInfo{
			Products: []struct {
				Name    string
				Version string
			}{
				{Name: "synq-clickhouse-client", Version: "1.0"},
			},
		},
	}

	if !conf.NoSsl {
		clickhouseOptions.TLS = &tls.Config{
			InsecureSkipVerify: true,
		}
	}

	db := clickhouse.OpenDB(clickhouseOptions)
	err := db.PingContext(ctx)
	if err != nil {
		return nil, exec.NewAuthError(err)
	}

	return &ClickhouseExecutor{db: sqlx.NewDb(db, "clickhouse"), conf: conf}, nil
}

func (e *ClickhouseExecutor) QueryRows(ctx context.Context, q string, args ...interface{}) (*sqlx.Rows, error) {
	ctx = EnrichClickhouseContext(ctx)
	rows, err := e.db.QueryxContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}

	return rows, nil
}

// EnrichClickhouseContext wraps the context with ClickHouse-specific options:
// - log_comment setting with JSON query context (when query context is present)
// - WithProgress/WithProfileInfo callbacks (when a querystats callback is registered)
// Creates or reuses a DriverStats accumulator so the Collector in stdsql helpers
// can merge the driver-specific stats at Finish() time.
func EnrichClickhouseContext(ctx context.Context) context.Context {
	var chOpts []clickhouse.QueryOption

	// Set log_comment for native query tagging in system.query_log
	if qc := querycontext.GetQueryContext(ctx); qc != nil {
		if tag := qc.FormatAsJSON(); tag != "" {
			chOpts = append(chOpts, clickhouse.WithSettings(clickhouse.Settings{
				"log_comment": tag,
			}))
		}
	}

	ds, ctx := querystats.GetOrCreateDriverStats(ctx)
	if ds != nil {
		queryID := uuid.New().String()
		ds.Set(querystats.QueryStats{QueryID: queryID})
		chOpts = append(chOpts,
			clickhouse.WithQueryID(queryID),
			clickhouse.WithProgress(func(p *clickhouse.Progress) {
				ds.Set(querystats.QueryStats{
					RowsRead:  querystats.Int64Ptr(int64(p.Rows)),
					BytesRead: querystats.Int64Ptr(int64(p.Bytes)),
				})
			}),
			clickhouse.WithProfileInfo(func(p *clickhouse.ProfileInfo) {
				ds.Set(querystats.QueryStats{
					RowsRead:  querystats.Int64Ptr(int64(p.Rows)),
					BytesRead: querystats.Int64Ptr(int64(p.Bytes)),
					Blocks:    querystats.Int64Ptr(int64(p.Blocks)),
				})
			}),
		)
	}

	if len(chOpts) == 0 {
		return ctx
	}
	return clickhouse.Context(ctx, chOpts...)
}

func (e *ClickhouseExecutor) Exec(ctx context.Context, q string) error {
	_, err := e.db.ExecContext(ctx, q)
	return err
}

func (e *ClickhouseExecutor) QueryRow(ctx context.Context, sql string, args ...interface{}) *sqlx.Row {
	return e.db.QueryRowxContext(ctx, sql, args...)
}
