package athena

import (
	"context"
	"fmt"

	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsglue "github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/getsynq/dwhsupport/logging"
	"github.com/getsynq/dwhsupport/scrapper"
	"golang.org/x/sync/errgroup"
)

type icebergPartitionTarget struct {
	schema string
	table  string
}

// QueryTableConstraints exposes partition columns as `PARTITION BY` constraint
// rows — the same shape BigQuery uses for time/range partitioning.
//
// Athena / Hive / Iceberg metastore has no traditional PK / FK / UNIQUE / CHECK
// concept; partitioning is the only constraint-shaped metadata Athena exposes
// that's worth surfacing.
//
// Sources, per row:
//
//  1. Glue `Table.PartitionKeys` — populated for Hive-style partitioned tables
//     (`PARTITIONED BY (col TYPE)`), free along with the metrics fetch.
//  2. Iceberg `$partitions` metadata table — Glue does NOT populate
//     `PartitionKeys` for Iceberg tables (the partition spec lives in the
//     `metadata.json` on S3, not in Glue), so we read the spec column names
//     from `SELECT * FROM "<db>"."<table>$partitions" LIMIT 1` when
//     `UseIcebergMetricsScan` is enabled. One Athena query per Iceberg table
//     (~$0.00005 each at the 10MB scan minimum).
//
// Iceberg transforms (`day(...)`, `bucket(N, ...)`, `truncate(L, ...)`) are
// flattened to their source column names — the per-column constraint row can't
// represent the transform. To recover the full transform string, enable
// `UseShowCreateTable` and parse the `PARTITIONED BY` clause from the DDL
// returned by `QuerySqlDefinitions`.
func (e *AthenaScrapper) QueryTableConstraints(ctx context.Context) ([]*scrapper.TableConstraintRow, error) {
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
		out             []*scrapper.TableConstraintRow
		icebergTargets  []icebergPartitionTarget
		icebergTableSet = map[string]bool{}
	)
	for _, dbName := range databases {
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
					continue
				}
				if len(t.PartitionKeys) > 0 {
					for i, k := range t.PartitionKeys {
						col := aws.ToString(k.Name)
						if col == "" {
							continue
						}
						out = append(out, &scrapper.TableConstraintRow{
							Instance:       instance,
							Database:       athenaCatalog,
							Schema:         dbName,
							Table:          name,
							ConstraintName: "partition_by",
							ColumnName:     col,
							ConstraintType: scrapper.ConstraintTypePartitionBy,
							ColumnPosition: int32(i),
						})
					}
				} else if e.conf.UseIcebergMetricsScan && isIcebergTable(t) {
					key := dbName + "." + name
					if !icebergTableSet[key] {
						icebergTableSet[key] = true
						icebergTargets = append(icebergTargets, icebergPartitionTarget{schema: dbName, table: name})
					}
				}
			}
			nextToken = resp.NextToken
			if nextToken == nil {
				break
			}
		}
	}
	if len(icebergTargets) > 0 {
		extra := e.fetchIcebergPartitionConstraints(ctx, icebergTargets, instance, athenaCatalog)
		out = append(out, extra...)
	}
	return out, nil
}

// fetchIcebergPartitionConstraints reads partition spec column names from each
// Iceberg table's `$partitions` metadata table. The partition spec is exposed
// as a single STRUCT column named `partition` with one field per partition
// column; we extract the field names from the row shape via QueryShape so we
// don't have to materialise any partition data.
func (e *AthenaScrapper) fetchIcebergPartitionConstraints(
	ctx context.Context,
	targets []icebergPartitionTarget,
	instance string,
	athenaCatalog string,
) []*scrapper.TableConstraintRow {
	var (
		mu  sync.Mutex
		out []*scrapper.TableConstraintRow
	)
	g, groupCtx := errgroup.WithContext(ctx)
	g.SetLimit(8)
	for _, target := range targets {
		if groupCtx.Err() != nil {
			break
		}
		target := target
		g.Go(func() error {
			cols, err := e.icebergPartitionColumns(groupCtx, target.schema, target.table)
			if err != nil {
				// Unpartitioned Iceberg tables surface as a missing `partition`
				// column on the `$partitions` metadata table — that's the
				// expected shape, not an error worth flagging.
				if !isUnpartitionedIcebergError(err) {
					logging.GetLogger(groupCtx).
						WithField("schema", target.schema).
						WithField("table", target.table).
						WithError(err).
						Warnf("athena: iceberg $partitions read failed")
				}
				return nil
			}
			if len(cols) == 0 {
				return nil
			}
			mu.Lock()
			defer mu.Unlock()
			for i, col := range cols {
				out = append(out, &scrapper.TableConstraintRow{
					Instance:       instance,
					Database:       athenaCatalog,
					Schema:         target.schema,
					Table:          target.table,
					ConstraintName: "partition_by",
					ColumnName:     col,
					ConstraintType: scrapper.ConstraintTypePartitionBy,
					ColumnPosition: int32(i),
				})
			}
			return nil
		})
	}
	_ = g.Wait()
	return out
}

// icebergPartitionColumns extracts the partition spec column names by reading
// `typeof(partition)` from the Iceberg `$partitions` metadata table. Athena's
// JDBC `ResultSetMetadata` flattens the STRUCT type to just `"row"`, so we
// have to ask Athena for the printed type string (e.g. `row(category varchar,
// day_ordered_at integer)`) and parse field names out of it. One Athena query
// per Iceberg table — the cost guard is `UseIcebergMetricsScan` upstream.
func (e *AthenaScrapper) icebergPartitionColumns(ctx context.Context, db, table string) ([]string, error) {
	q := fmt.Sprintf(
		`SELECT typeof(partition) AS t FROM "%s"."%s$partitions" LIMIT 1`,
		strings.ReplaceAll(db, `"`, `""`),
		strings.ReplaceAll(table, `"`, `""`),
	)
	type row struct {
		T string `db:"t"`
	}
	var rows []row
	if err := e.executor.Select(ctx, &rows, q); err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	return parseIcebergPartitionStructFields(rows[0].T), nil
}

func isUnpartitionedIcebergError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "COLUMN_NOT_FOUND") && strings.Contains(msg, "'partition'")
}

// parseIcebergPartitionStructFields extracts field names from Athena's struct
// type signature, e.g. `row(category varchar, day_ordered_at integer)`. We
// don't need a real parser — pull tokens between '(' / ',' / ')' and take the
// first identifier of each.
func parseIcebergPartitionStructFields(t string) []string {
	open := strings.Index(t, "(")
	if open < 0 {
		return nil
	}
	depth := 0
	end := -1
	for i := open; i < len(t); i++ {
		switch t[i] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				end = i
			}
		}
		if end >= 0 {
			break
		}
	}
	if end < 0 {
		return nil
	}
	body := t[open+1 : end]

	var (
		fields []string
		buf    strings.Builder
		d      int
	)
	flush := func() {
		raw := strings.TrimSpace(buf.String())
		buf.Reset()
		if raw == "" {
			return
		}
		// Each field is `<name> <type>`; we want the first whitespace-separated token.
		if sp := strings.IndexAny(raw, " \t"); sp > 0 {
			raw = raw[:sp]
		}
		fields = append(fields, raw)
	}
	for i := 0; i < len(body); i++ {
		c := body[i]
		switch c {
		case '(':
			d++
			buf.WriteByte(c)
		case ')':
			d--
			buf.WriteByte(c)
		case ',':
			if d == 0 {
				flush()
				continue
			}
			buf.WriteByte(c)
		default:
			buf.WriteByte(c)
		}
	}
	flush()
	return fields
}
