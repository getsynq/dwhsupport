package snowflake

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/getsynq/dwhsupport/logging"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scope"
	"github.com/pkg/errors"
	"github.com/samber/lo"
	"golang.org/x/sync/errgroup"
)

var tablesQuery = `
	select
		t.table_catalog as "database",
        t.table_schema as "schema",
        t.table_name as "table",
        IFF(t.is_dynamic = 'YES', 'DYNAMIC TABLE', t.table_type) as "table_type",
        NVL2(t.comment, t.comment, '') as "description",
        (table_type='VIEW' OR table_type='MATERIALIZED VIEW') as "is_view",
        table_type='BASE TABLE' as "is_table"
	from
		%s.information_schema.tables as t
	where
		UPPER(t.table_schema) NOT IN ('INFORMATION_SCHEMA', 'SYSADMIN')
		/* SYNQ_SCOPE_FILTER */
	`

func (e *SnowflakeScrapper) QueryTables(origCtx context.Context, opts ...scrapper.QueryTablesOption) ([]*scrapper.TableRow, error) {
	var finalResults []*scrapper.TableRow
	var m sync.Mutex

	databasesToQuery, err := e.GetDatabasesToQuery(origCtx)
	if err != nil {
		return nil, err
	}

	g, groupCtx := errgroup.WithContext(origCtx)
	g.SetLimit(8)

	for _, database := range databasesToQuery {
		select {
		case <-groupCtx.Done():
			return nil, groupCtx.Err()
		default:
		}

		g.Go(
			func() error {

				var tmpResults []*scrapper.TableRow

				query := scope.AppendScopeConditions(origCtx, fmt.Sprintf(tablesQuery, database), "", "t.table_schema", "t.table_name")
				rows, err := e.executor.QueryRows(groupCtx, query)
				if err != nil {
					if isSharedDatabaseUnavailableError(err) {
						logging.GetLogger(groupCtx).WithField("database", database).WithError(err).
							Warn("Shared database is no longer available, skipping")
						return nil
					}
					return errors.Wrapf(err, "failed to query tables for database %s", database)
				}
				defer rows.Close()

				for rows.Next() {
					result := scrapper.TableRow{}
					if err := rows.StructScan(&result); err != nil {
						return errors.Wrapf(err, "failed to scan table row for database %s", database)
					}
					result.Instance = e.conf.Account
					if result.Description != nil && *result.Description == "" {
						result.Description = nil
					}
					tmpResults = append(tmpResults, &result)
				}

				streamRows, err := e.showStreamsInDatabase(groupCtx, database)
				if err == nil {
					for _, streamRow := range streamRows {

						tmpResults = append(
							tmpResults, &scrapper.TableRow{
								Instance:  e.conf.Account,
								Database:  streamRow.DatabaseName,
								Schema:    streamRow.SchemaName,
								Table:     streamRow.Name,
								TableType: "STREAM",
								IsView:    false,
								IsTable:   true,
								Options: map[string]interface{}{
									"table_name": streamRow.TableName,
								},
								Description: lo.EmptyableToPtr(streamRow.Comment),
								Annotations: []*scrapper.Annotation{
									{
										AnnotationName:  "stream_type",
										AnnotationValue: streamRow.Type,
									},
									{
										AnnotationName:  "stream_mode",
										AnnotationValue: streamRow.Mode,
									},
									{
										AnnotationName:  "stream_stale",
										AnnotationValue: streamRow.Stale,
									},
								},
							},
						)

					}
				} else {
					logging.GetLogger(groupCtx).WithField("database", database).WithError(err).Error("SHOW STREAMS IN DATABASE failed")
				}

				m.Lock()
				defer m.Unlock()
				finalResults = append(finalResults, tmpResults...)

				return nil
			},
		)
	}

	err = g.Wait()
	if err != nil {
		return nil, err
	}

	// Post-filter for SHOW STREAMS results which bypass SQL scope conditions.
	return scope.FilterRows(finalResults, scope.GetScope(origCtx)), nil
}

// ShowStreamsRow represents the structure of a row returned by SHOW STREAMS command
type ShowStreamsRow struct {
	// CreatedOn is the timestamp when the stream was created
	CreatedOn string
	// Name is the name of the stream
	Name string
	// DatabaseName is the database containing the stream
	DatabaseName string
	// SchemaName is the schema containing the stream
	SchemaName string
	// Owner is the owner of the stream
	Owner string
	// Comment is the comment/description for the stream
	Comment string
	// TableName is the name of the source table the stream tracks
	TableName string
	// SourceType is the type of source object (Table, View, etc.)
	SourceType string
	// BaseTables lists the base tables the stream tracks
	BaseTables string
	// Type is the stream type (DELTA, etc.)
	Type string
	// Stale indicates whether the stream is stale (true/false)
	Stale string
	// Mode is the stream mode: DEFAULT (standard/delta), APPEND_ONLY, or INSERT_ONLY
	Mode string
	// StaleAfter is the timestamp when the stream becomes stale (empty if NULL)
	StaleAfter string
	// InvalidReason provides the reason if the stream is invalid (empty if NULL)
	InvalidReason string
	// OwnerRoleType is the type of owner role
	OwnerRoleType string
}

func (e *SnowflakeScrapper) showStreamsInDatabase(ctx context.Context, database string) ([]*ShowStreamsRow, error) {
	var results []*ShowStreamsRow

	rows, err := e.executor.QueryRows(ctx, fmt.Sprintf("SHOW STREAMS IN DATABASE %s", database))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Use MapScan rather than StructScan: Snowflake's SHOW STREAMS returns NULL for
	// several columns (stale_after, invalid_reason, ...) which cannot be scanned into
	// a plain `string`. MapScan tolerates NULL and is also resilient to column set
	// changes across Snowflake versions.
	for rows.Next() {
		tmp := map[string]any{}
		if err := rows.MapScan(tmp); err != nil {
			return nil, err
		}
		results = append(results, &ShowStreamsRow{
			CreatedOn:     mapStr(tmp, "created_on"),
			Name:          mapStr(tmp, "name"),
			DatabaseName:  mapStr(tmp, "database_name"),
			SchemaName:    mapStr(tmp, "schema_name"),
			Owner:         mapStr(tmp, "owner"),
			Comment:       mapStr(tmp, "comment"),
			TableName:     mapStr(tmp, "table_name"),
			SourceType:    mapStr(tmp, "source_type"),
			BaseTables:    mapStr(tmp, "base_tables"),
			Type:          mapStr(tmp, "type"),
			Stale:         mapStr(tmp, "stale"),
			Mode:          mapStr(tmp, "mode"),
			StaleAfter:    mapStr(tmp, "stale_after"),
			InvalidReason: mapStr(tmp, "invalid_reason"),
			OwnerRoleType: mapStr(tmp, "owner_role_type"),
		})
	}

	return results, nil
}

func mapStr(m map[string]any, key string) string {
	v, ok := m[key]
	if !ok || v == nil {
		return ""
	}
	switch s := v.(type) {
	case string:
		return s
	case []byte:
		return string(s)
	default:
		return fmt.Sprint(v)
	}
}

func (e *SnowflakeScrapper) showShares(ctx context.Context) ([]*ShareDesc, error) {
	rows, err := e.executor.QueryRows(ctx, "SHOW SHARES")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var shares []*ShareDesc
	for rows.Next() {
		share := &ShareDesc{}
		tmp := map[string]interface{}{}
		if err := rows.MapScan(tmp); err != nil {
			return nil, err
		}
		share.Name = tmp["name"].(string)
		share.Kind = tmp["kind"].(string)
		share.OwnerAccount = tmp["owner_account"].(string)
		share.DatabaseName = tmp["database_name"].(string)
		share.To = tmp["to"].(string)
		share.Owner = tmp["owner"].(string)
		share.Comment = tmp["comment"].(string)
		share.ListingGlobalName = tmp["listing_global_name"].(string)

		if share.OwnerAccount == "SNOWFLAKE" && share.Name == "ACCOUNT_USAGE" {
			continue
		}
		if strings.HasPrefix(share.OwnerAccount, "SFSALESSHARED.") {
			continue
		}
		if strings.HasPrefix(share.OwnerAccount, "WEATHERSOURCE.") {
			continue
		}

		if share.Kind == "INBOUND" {
			if err := e.executor.Select(
				ctx, &share.Objects, fmt.Sprintf("DESCRIBE SHARE %s.%s", share.OwnerAccount, share.Name),
			); err != nil {
				return nil, err
			}
		}
		if share.Kind == "OUTBOUND" {
			if err := e.executor.Select(ctx, &share.Objects, fmt.Sprintf("DESCRIBE SHARE %s", share.Name)); err != nil {
				return nil, err
			}
		}

		shares = append(shares, share)
	}
	if len(shares) > 0 {
		logging.GetLogger(ctx).WithField("shares", shares).Info("show shares")
	} else {
		logging.GetLogger(ctx).Info("no shares")
	}
	return shares, nil
}

type ShareObject struct {
	Kind     string    `db:"kind"      json:"kind"`
	Name     string    `db:"name"      json:"name"`
	SharedOn time.Time `db:"shared_on" json:"shared_on"`
}

type ShareDesc struct {
	Name              string         `db:"name"                json:"name"`
	Kind              string         `db:"kind"                json:"kind"`
	OwnerAccount      string         `db:"owner_account"       json:"owner_account"`
	DatabaseName      string         `db:"database_name"       json:"database_name"`
	To                string         `db:"to"                  json:"to"`
	Owner             string         `db:"owner"               json:"owner"`
	Comment           string         `db:"comment"             json:"comment"`
	ListingGlobalName string         `db:"listing_global_name" json:"listing_global_name"`
	Objects           []*ShareObject `                         json:"objects"`
}

func (d *ShareDesc) String() string {
	return fmt.Sprintf(
		"Name: %s, Kind: %s, OwnerAccount: %s, DatabaseName: %s, To: %s, Owner: %s, Comment: %s, ListingGlobalName: %s", d.Name, d.Kind,
		d.OwnerAccount, d.DatabaseName, d.To, d.Owner, d.Comment, d.ListingGlobalName,
	)
}
