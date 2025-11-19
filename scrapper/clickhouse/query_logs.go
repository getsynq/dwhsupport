package clickhouse

import (
	"context"
	_ "embed"
	"io"
	"net"
	"strings"
	"time"

	dwhexec "github.com/getsynq/dwhsupport/exec"
	dwhexecclickhouse "github.com/getsynq/dwhsupport/exec/clickhouse"
	"github.com/getsynq/dwhsupport/querylogs"
	"github.com/getsynq/dwhsupport/scrapper"
)

//go:embed query_logs.sql
var queryLogsSql string

type ClickhouseQueryLogSchema struct {
	QueryType                  string    `db:"type"`
	EventTimeMicroseconds      time.Time `db:"event_time_microseconds"`
	QueryStartTimeMicroseconds time.Time `db:"query_start_time_microseconds"`

	// IO Information
	ReadRows  uint64 `db:"read_rows"`
	ReadBytes uint64 `db:"read_bytes"`

	WrittenRows  uint64 `db:"written_rows"`
	WrittenBytes uint64 `db:"written_bytes"`

	ResultRows  uint64 `db:"result_rows"`
	ResultBytes uint64 `db:"result_bytes"`

	// Query Information
	MemoryUsage     uint64   `db:"memory_usage"`
	CurrentDatabase string   `db:"current_database"`
	Query           string   `db:"normalized_query"`
	QueryKind       string   `db:"query_kind"`
	Databases       []string `db:"databases"`
	Tables          []string `db:"tables"`
	Columns         []string `db:"columns"`
	Projections     []string `db:"projections"`
	Views           []string `db:"views"`
	ExceptionCode   int32    `db:"exception_code"`
	Exception       string   `db:"exception"`
	StackTrace      string   `db:"stack_trace"`

	// Client Information
	InitialUser                       string    `db:"initial_user"`
	InitialQueryId                    string    `db:"initial_query_id"`
	InitialAddress                    *net.IP   `db:"initial_address"`
	InitialPort                       uint16    `db:"initial_port"`
	InitialQueryStartTimeMicroseconds time.Time `db:"initial_query_start_time_microseconds"`
	OsUser                            string    `db:"os_user"`
	ClientHostname                    string    `db:"client_hostname"`
	ClientName                        string    `db:"client_name"`
	ClientRevision                    uint32    `db:"client_revision"`
	ClientRevisionMajor               uint32    `db:"client_version_major"`
	ClientRevisionMinor               uint32    `db:"client_version_minor"`
	ClientRevisionPatch               uint32    `db:"client_version_patch"`
	DistributedDepth                  uint64    `db:"distributed_depth"`
}

type clickhouseQueryLogIterator struct {
	buffer   []*querylogs.QueryLog
	index    int
	rowsChan chan *ClickhouseQueryLogSchema
	errChan  chan error
	closed   bool
}

func (s *ClickhouseScrapper) FetchQueryLogs(ctx context.Context, from, to time.Time) (querylogs.QueryLogIterator, error) {
	iter := &clickhouseQueryLogIterator{
		rowsChan: make(chan *ClickhouseQueryLogSchema, 100), // Buffer for performance
		errChan:  make(chan error, 1),
	}

	// Start fetching in background
	go func() {
		defer close(iter.rowsChan)
		defer close(iter.errChan)

		querier := dwhexecclickhouse.NewQuerier[ClickhouseQueryLogSchema](s.Executor())
		err := querier.QueryAndProcessMany(
			ctx,
			queryLogsSql,
			func(ctx context.Context, rows []*ClickhouseQueryLogSchema) error {
				for _, row := range rows {
					select {
					case iter.rowsChan <- row:
					case <-ctx.Done():
						return ctx.Err()
					}
				}
				return nil
			},
			dwhexec.WithArgs[ClickhouseQueryLogSchema](from, to),
		)

		if err != nil {
			select {
			case iter.errChan <- err:
			default:
			}
		}
	}()

	return iter, nil
}

func (it *clickhouseQueryLogIterator) Next(ctx context.Context) (*querylogs.QueryLog, error) {
	if it.closed {
		return nil, io.EOF
	}

	// Return from buffer first
	if it.index < len(it.buffer) {
		log := it.buffer[it.index]
		it.index++
		return log, nil
	}

	// Reset buffer
	it.buffer = nil
	it.index = 0

	// Try to get next row
	select {
	case row, ok := <-it.rowsChan:
		if !ok {
			// Channel closed - check for error
			select {
			case err := <-it.errChan:
				// Don't auto-close on error - caller might want to retry
				return nil, err
			default:
				// Normal completion - auto-close
				it.Close()
				return nil, io.EOF
			}
		}

		log, err := convertClickhouseRowToQueryLog(row)
		if err != nil {
			// Defensive: log conversion error but continue (don't crash entire ingestion)
			// Return the error so caller can decide whether to continue
			return nil, err
		}

		return log, nil

	case <-ctx.Done():
		it.Close()
		return nil, ctx.Err()
	}
}

func (it *clickhouseQueryLogIterator) Close() error {
	if it.closed {
		return nil
	}
	it.closed = true

	// Drain channels to prevent goroutine leaks
	go func() {
		for range it.rowsChan {
			// Drain
		}
	}()

	return nil
}

func convertClickhouseRowToQueryLog(row *ClickhouseQueryLogSchema) (*querylogs.QueryLog, error) {
	// Parse tables array into native lineage
	var nativeLineage *querylogs.NativeLineage
	if len(row.Tables) > 0 {
		inputTables := make([]scrapper.DwhFqn, 0, len(row.Tables))

		for _, t := range row.Tables {
			// Parse "schema.table" format
			var schema, table string
			split := strings.Split(t, ".")
			if len(split) == 1 {
				schema = ""
				table = split[0]
			} else {
				schema = split[0]
				table = strings.Join(split[1:], ".")
			}

			// Filter temporary tables like "`.inner_id.e16b8c51-6afc-4d0f-877c-76e4f75b39d9"
			if strings.HasPrefix(table, "`.") {
				continue
			}

			inputTables = append(inputTables, scrapper.DwhFqn{
				DatabaseName: schema,
				SchemaName:   "",     // ClickHouse doesn't have schema concept in the same way
				ObjectName:   table,
			})
		}

		if len(inputTables) > 0 {
			nativeLineage = &querylogs.NativeLineage{
				InputTables:  inputTables,
				OutputTables: nil, // ClickHouse doesn't provide output tables in query_log
			}
		}
	}

	// Determine status
	status := "SUCCESS"
	if row.Exception != "" {
		status = "FAILED"
	}

	// Build metadata with all ClickHouse-specific fields
	metadata := map[string]interface{}{
		"query_type":        row.QueryType,
		"read_rows":         row.ReadRows,
		"read_bytes":        row.ReadBytes,
		"written_rows":      row.WrittenRows,
		"written_bytes":     row.WrittenBytes,
		"result_rows":       row.ResultRows,
		"result_bytes":      row.ResultBytes,
		"memory_usage":      row.MemoryUsage,
		"databases":         row.Databases,
		"columns":           row.Columns,
		"projections":       row.Projections,
		"views":             row.Views,
		"exception_code":    row.ExceptionCode,
		"stack_trace":       row.StackTrace,
		"os_user":           row.OsUser,
		"client_hostname":   row.ClientHostname,
		"client_name":       row.ClientName,
		"client_revision":   row.ClientRevision,
		"distributed_depth": row.DistributedDepth,
	}

	if row.InitialAddress != nil {
		metadata["initial_address"] = row.InitialAddress.String()
		metadata["initial_port"] = row.InitialPort
	}

	return &querylogs.QueryLog{
		CreatedAt: row.QueryStartTimeMicroseconds,
		QueryID:   row.InitialQueryId,
		SQL:       row.Query,
		DwhContext: &querylogs.DwhContext{
			Database: row.CurrentDatabase,
			User:     row.InitialUser,
		},
		QueryType:                row.QueryKind,
		Status:                   status,
		Metadata:                 metadata,
		SqlObfuscationMode:       querylogs.ObfuscationNone, // Will be set by wrapper if needed
		IsParsable:               len(row.Query) > 0 && len(row.Query) < 100000,
		HasCompleteNativeLineage: false, // ClickHouse only provides input tables, not complete lineage
		NativeLineage:            nativeLineage,
	}, nil
}
