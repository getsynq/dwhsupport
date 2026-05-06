package athena

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsglue "github.com/aws/aws-sdk-go-v2/service/glue"
	gluetypes "github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/getsynq/dwhsupport/logging"
	"github.com/getsynq/dwhsupport/scrapper"
	"golang.org/x/sync/errgroup"
)

// QueryTableMetrics returns row counts, total size in bytes, and last-updated
// timestamps for tables visible to the configured Glue catalog.
//
// Sources, in order of priority per row:
//
//  1. Glue table parameters (Hive table-level statistics): `numRows`, `totalSize`,
//     `transient_lastDdlTime`. Populated by `ANALYZE TABLE COMPUTE STATISTICS`
//     or by Glue crawlers with stats enabled. Free — one paginated `glue:GetTables`
//     call per Glue database, no Athena query cost.
//  2. Iceberg `$files` metadata table aggregate (only when `UseIcebergMetricsScan`
//     is enabled and the Glue parameters didn't already supply row count + size).
//     One Athena query per Iceberg table (~$0.00005 each at the 10MB scan minimum).
//
// Tables with no usable stats from either source are skipped — we'd rather omit
// a row than emit zeros that downstream consumers treat as a real "0 rows".
//
// `lastFetchTime` is honoured against Glue's `UpdateTime`: tables that haven't
// changed since the last fetch are skipped entirely, including their Iceberg scan.
func (e *AthenaScrapper) QueryTableMetrics(ctx context.Context, lastFetchTime time.Time) ([]*scrapper.TableMetricsRow, error) {
	glueClient := e.executor.GlueClient()
	if glueClient == nil {
		return nil, nil
	}

	databases, err := e.listGlueDatabases(ctx, glueClient)
	if err != nil {
		return nil, fmt.Errorf("athena: list glue databases: %w", err)
	}

	catalogID := e.executor.AccountID()
	instance := e.executor.Instance()
	athenaCatalog := e.executor.Catalog()

	var (
		out            []*scrapper.TableMetricsRow
		icebergPending []*scrapper.TableMetricsRow
	)

	for _, dbName := range databases {
		// Filter at the schema level before paging tables — saves the Glue
		// call entirely for excluded databases.
		if e.conf.Scope != nil && !e.conf.Scope.IsSchemaAccepted(athenaCatalog, dbName) {
			continue
		}

		var nextToken *string
		for {
			resp, err := glueClient.GetTables(ctx, &awsglue.GetTablesInput{
				CatalogId:    aws.String(catalogID),
				DatabaseName: aws.String(dbName),
				NextToken:    nextToken,
			})
			if err != nil {
				return nil, fmt.Errorf("athena: glue:GetTables %q: %w", dbName, err)
			}
			for _, t := range resp.TableList {
				name := aws.ToString(t.Name)
				if e.conf.Scope != nil && !e.conf.Scope.IsObjectAccepted(athenaCatalog, dbName, name) {
					continue
				}
				if isGlueView(t) {
					// information_schema reports views in QueryTables; metrics don't apply.
					continue
				}
				updatedAt := glueUpdateTime(t)
				if !lastFetchTime.IsZero() && updatedAt != nil && !updatedAt.After(lastFetchTime) {
					continue
				}

				row := &scrapper.TableMetricsRow{
					Instance:  instance,
					Database:  athenaCatalog,
					Schema:    dbName,
					Table:     name,
					UpdatedAt: updatedAt,
				}
				if rowCount, ok := parseInt64Param(t.Parameters, "numRows"); ok && rowCount >= 0 {
					row.RowCount = &rowCount
				}
				if size, ok := parseInt64Param(t.Parameters, "totalSize"); ok && size >= 0 {
					row.SizeBytes = &size
				}

				if e.conf.UseIcebergMetricsScan && (row.RowCount == nil || row.SizeBytes == nil) && isIcebergTable(t) {
					icebergPending = append(icebergPending, row)
				}
				out = append(out, row)
			}
			nextToken = resp.NextToken
			if nextToken == nil {
				break
			}
		}
	}

	if len(icebergPending) > 0 {
		e.fillIcebergMetrics(ctx, icebergPending)
	}

	// Drop rows where we have no metric to report — no row count, no size.
	// An UpdatedAt-only row would mislead downstream consumers.
	filtered := out[:0]
	for _, r := range out {
		if r.RowCount == nil && r.SizeBytes == nil && r.UpdatedAt == nil {
			continue
		}
		filtered = append(filtered, r)
	}
	return filtered, nil
}

func (e *AthenaScrapper) listGlueDatabases(ctx context.Context, glueClient *awsglue.Client) ([]string, error) {
	catalogID := e.executor.AccountID()
	var (
		names     []string
		nextToken *string
	)
	for {
		resp, err := glueClient.GetDatabases(ctx, &awsglue.GetDatabasesInput{
			CatalogId: aws.String(catalogID),
			NextToken: nextToken,
		})
		if err != nil {
			return nil, err
		}
		for _, d := range resp.DatabaseList {
			names = append(names, aws.ToString(d.Name))
		}
		nextToken = resp.NextToken
		if nextToken == nil {
			break
		}
	}
	return names, nil
}

// fillIcebergMetrics fans out one Athena query per Iceberg table that's missing
// row count or size from Glue parameters. Failures are logged but not fatal —
// a single table's metrics scan failing should not abort the entire fetch.
func (e *AthenaScrapper) fillIcebergMetrics(ctx context.Context, rows []*scrapper.TableMetricsRow) {
	g, groupCtx := errgroup.WithContext(ctx)
	g.SetLimit(8)

	var mu sync.Mutex
	for _, row := range rows {
		if groupCtx.Err() != nil {
			break
		}
		row := row
		g.Go(func() error {
			rc, sz, err := e.scanIcebergFiles(groupCtx, row.Schema, row.Table)
			if err != nil {
				logging.GetLogger(groupCtx).
					WithField("table_fqn", row.TableFqn()).
					WithError(err).
					Warnf("athena: iceberg $files scan failed")
				return nil
			}
			mu.Lock()
			if rc != nil && row.RowCount == nil {
				row.RowCount = rc
			}
			if sz != nil && row.SizeBytes == nil {
				row.SizeBytes = sz
			}
			mu.Unlock()
			return nil
		})
	}
	_ = g.Wait()
}

// scanIcebergFiles runs `SELECT SUM(record_count), SUM(file_size_in_bytes)
// FROM "<db>"."<table>$files"` against Athena. Iceberg's `$files` metadata
// table lists every active data file in the current snapshot — summing it
// gives an exact row count and on-disk size without reading any data files.
func (e *AthenaScrapper) scanIcebergFiles(ctx context.Context, db, table string) (*int64, *int64, error) {
	q := fmt.Sprintf(
		`SELECT SUM(record_count) AS row_count, SUM(file_size_in_bytes) AS size_bytes FROM "%s"."%s$files"`,
		strings.ReplaceAll(db, `"`, `""`),
		strings.ReplaceAll(table, `"`, `""`),
	)
	type icebergRow struct {
		RowCount  *int64 `db:"row_count"`
		SizeBytes *int64 `db:"size_bytes"`
	}
	var rows []icebergRow
	if err := e.executor.Select(ctx, &rows, q); err != nil {
		return nil, nil, err
	}
	if len(rows) == 0 {
		return nil, nil, nil
	}
	return rows[0].RowCount, rows[0].SizeBytes, nil
}

func isGlueView(t gluetypes.Table) bool {
	switch strings.ToUpper(aws.ToString(t.TableType)) {
	case "VIRTUAL_VIEW", "VIEW", "MATERIALIZED_VIEW":
		return true
	}
	return false
}

// isIcebergTable detects Iceberg tables via the `table_type` parameter Athena
// sets on the Glue Table when it's created via `TBLPROPERTIES('table_type'='ICEBERG')`,
// and via the `metadata_location` parameter that the Iceberg writer maintains.
func isIcebergTable(t gluetypes.Table) bool {
	if v, ok := t.Parameters["table_type"]; ok && strings.EqualFold(v, "ICEBERG") {
		return true
	}
	if _, ok := t.Parameters["metadata_location"]; ok {
		return true
	}
	return false
}

func parseInt64Param(params map[string]string, key string) (int64, bool) {
	if params == nil {
		return 0, false
	}
	raw, ok := params[key]
	if !ok || raw == "" {
		return 0, false
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, false
	}
	return v, true
}

// glueUpdateTime prefers the Glue `UpdateTime` (last metadata mutation) and
// falls back to `CreateTime`. For Iceberg tables both reflect the most recent
// commit; for Hive externals it tracks the last DDL.
func glueUpdateTime(t gluetypes.Table) *time.Time {
	if t.UpdateTime != nil {
		ts := t.UpdateTime.UTC()
		return &ts
	}
	if t.CreateTime != nil {
		ts := t.CreateTime.UTC()
		return &ts
	}
	if raw, ok := parseInt64Param(t.Parameters, "transient_lastDdlTime"); ok && raw > 0 {
		ts := time.Unix(raw, 0).UTC()
		return &ts
	}
	return nil
}
