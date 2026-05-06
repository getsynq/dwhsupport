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

				if e.conf.UseIcebergMetricsScan && isIcebergTable(t) {
					// Always queue Iceberg tables for the metadata scan when
					// the flag is on — even if Glue already had row count and
					// size, we still want the precise commit timestamp from
					// $snapshots for accurate data freshness. The combined
					// $files + $snapshots query is one Athena call either way.
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

// fillIcebergMetrics fans out one Athena query per Iceberg table to read row
// count, on-disk size, and the latest snapshot commit timestamp from the
// table's metadata tables. Failures are logged but not fatal — a single
// table's scan failing should not abort the entire fetch.
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
			rc, sz, committedAt, err := e.scanIcebergMetadata(groupCtx, row.Schema, row.Table)
			if err != nil {
				logging.GetLogger(groupCtx).
					WithField("table_fqn", row.TableFqn()).
					WithError(err).
					Warnf("athena: iceberg metadata scan failed")
				return nil
			}
			mu.Lock()
			if rc != nil && row.RowCount == nil {
				row.RowCount = rc
			}
			if sz != nil && row.SizeBytes == nil {
				row.SizeBytes = sz
			}
			if committedAt != nil {
				// Snapshot commit time is the truth for Iceberg data freshness:
				// every INSERT / MERGE / UPDATE writes a snapshot. Glue's
				// UpdateTime usually tracks this too (the metadata_location
				// pointer flips on each commit) but the snapshot timestamp is
				// authoritative — prefer it unconditionally when available.
				ts := committedAt.UTC()
				row.UpdatedAt = &ts
			}
			mu.Unlock()
			return nil
		})
	}
	_ = g.Wait()
}

// scanIcebergMetadata reads row count + total size from `$files` and the
// latest commit timestamp from `$snapshots` in a single Athena query.
// Iceberg's `$files` metadata table lists every active data file in the
// current snapshot; `$snapshots` lists every commit (INSERT, MERGE, UPDATE,
// DELETE) with its `committed_at`. Both are cheap metadata reads — neither
// touches the data files.
func (e *AthenaScrapper) scanIcebergMetadata(ctx context.Context, db, table string) (*int64, *int64, *time.Time, error) {
	dbQuoted := strings.ReplaceAll(db, `"`, `""`)
	tQuoted := strings.ReplaceAll(table, `"`, `""`)
	q := fmt.Sprintf(
		`SELECT
			(SELECT SUM(record_count)        FROM "%s"."%s$files")     AS row_count,
			(SELECT SUM(file_size_in_bytes)  FROM "%s"."%s$files")     AS size_bytes,
			(SELECT MAX(committed_at)        FROM "%s"."%s$snapshots") AS last_committed_at`,
		dbQuoted, tQuoted, dbQuoted, tQuoted, dbQuoted, tQuoted,
	)
	type icebergRow struct {
		RowCount        *int64     `db:"row_count"`
		SizeBytes       *int64     `db:"size_bytes"`
		LastCommittedAt *time.Time `db:"last_committed_at"`
	}
	var rows []icebergRow
	if err := e.executor.Select(ctx, &rows, q); err != nil {
		return nil, nil, nil, err
	}
	if len(rows) == 0 {
		return nil, nil, nil, nil
	}
	return rows[0].RowCount, rows[0].SizeBytes, rows[0].LastCommittedAt, nil
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
