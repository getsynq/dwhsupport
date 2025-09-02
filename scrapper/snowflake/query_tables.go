package snowflake

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/getsynq/dwhsupport/logging"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/samber/lo"
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
	`

func (e *SnowflakeScrapper) QueryTables(ctx context.Context) ([]*scrapper.TableRow, error) {
	var results []*scrapper.TableRow

	allDatabases, err := e.allAllowedDatabases(ctx)
	if err != nil {
		return nil, err
	}

	existingDbs := map[string]bool{}
	for _, database := range allDatabases {
		existingDbs[database.Name] = true
	}
	for _, database := range e.conf.Databases {
		if !existingDbs[database] {
			continue
		}
		rows, err := e.executor.GetDb().QueryxContext(ctx, fmt.Sprintf(tablesQuery, database))
		if err != nil {
			return nil, err
		}

		for rows.Next() {
			result := scrapper.TableRow{}
			if err := rows.StructScan(&result); err != nil {
				return nil, err
			}
			result.Instance = e.conf.Account
			if result.Description != nil && *result.Description == "" {
				result.Description = nil
			}
			results = append(results, &result)
		}

		streamRows, err := e.showStreamsInDatabase(ctx, database)
		if err == nil {
			for _, streamRow := range streamRows {

				results = append(results, &scrapper.TableRow{
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
				})

			}
		} else {
			logging.GetLogger(ctx).WithField("database", database).WithError(err).Error("SHOW STREAMS IN DATABASE failed")
		}
	}

	return results, nil
}

// ShowStreamsRow represents the structure of a row returned by SHOW STREAMS command
type ShowStreamsRow struct {
	// CreatedOn is the timestamp when the stream was created
	CreatedOn string `db:"created_on"`
	// Name is the name of the stream
	Name string `db:"name"`
	// DatabaseName is the database containing the stream
	DatabaseName string `db:"database_name"`
	// SchemaName is the schema containing the stream
	SchemaName string `db:"schema_name"`
	// Owner is the owner of the stream
	Owner string `db:"owner"`
	// Comment is the comment/description for the stream
	Comment string `db:"comment"`
	// TableName is the name of the source table the stream tracks
	TableName string `db:"table_name"`
	// SourceType is the type of source object (Table, View, etc.)
	SourceType string `db:"source_type"`
	// BaseTables lists the base tables the stream tracks
	BaseTables string `db:"base_tables"`
	// Type is the stream type (DELTA, etc.)
	Type string `db:"type"`
	// Stale indicates whether the stream is stale (true/false)
	Stale string `db:"stale"`
	// Mode is the stream mode: DEFAULT (standard/delta), APPEND_ONLY, or INSERT_ONLY
	Mode string `db:"mode"`
	// StaleAfter is the timestamp when the stream becomes stale
	StaleAfter string `db:"stale_after"`
	// InvalidReason provides the reason if the stream is invalid
	InvalidReason string `db:"invalid_reason"`
	// OwnerRoleType is the type of owner role
	OwnerRoleType string `db:"owner_role_type"`
}

func (e *SnowflakeScrapper) showStreamsInDatabase(ctx context.Context, database string) ([]*ShowStreamsRow, error) {
	var results []*ShowStreamsRow

	rows, err := e.executor.GetDb().QueryxContext(ctx, fmt.Sprintf("SHOW STREAMS IN DATABASE %s", database))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		result := &ShowStreamsRow{}
		if err := rows.StructScan(result); err != nil {
			return nil, err
		}
		results = append(results, result)
	}

	return results, nil
}

func (e *SnowflakeScrapper) showShares(ctx context.Context) ([]*ShareDesc, error) {
	rows, err := e.executor.GetDb().QueryxContext(ctx, "SHOW SHARES")
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
			if err := e.executor.GetDb().SelectContext(ctx, &share.Objects, fmt.Sprintf("DESCRIBE SHARE %s.%s", share.OwnerAccount, share.Name)); err != nil {
				return nil, err
			}
		}
		if share.Kind == "OUTBOUND" {
			if err := e.executor.GetDb().SelectContext(ctx, &share.Objects, fmt.Sprintf("DESCRIBE SHARE %s", share.Name)); err != nil {
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
	Kind     string    `db:"kind" json:"kind"`
	Name     string    `db:"name" json:"name"`
	SharedOn time.Time `db:"shared_on" json:"shared_on"`
}

type ShareDesc struct {
	Name              string         `db:"name" json:"name"`
	Kind              string         `db:"kind" json:"kind"`
	OwnerAccount      string         `db:"owner_account" json:"owner_account"`
	DatabaseName      string         `db:"database_name" json:"database_name"`
	To                string         `db:"to" json:"to"`
	Owner             string         `db:"owner" json:"owner"`
	Comment           string         `db:"comment" json:"comment"`
	ListingGlobalName string         `db:"listing_global_name" json:"listing_global_name"`
	Objects           []*ShareObject `json:"objects"`
}

func (d *ShareDesc) String() string {
	return fmt.Sprintf("Name: %s, Kind: %s, OwnerAccount: %s, DatabaseName: %s, To: %s, Owner: %s, Comment: %s, ListingGlobalName: %s", d.Name, d.Kind, d.OwnerAccount, d.DatabaseName, d.To, d.Owner, d.Comment, d.ListingGlobalName)
}
