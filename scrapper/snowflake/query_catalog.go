package snowflake

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/getsynq/dwhsupport/logging"
	"github.com/getsynq/dwhsupport/scrapper"
)

var catalogQuery = `
	select 
	    CURRENT_ORGANIZATION_NAME() || '-' || CURRENT_ACCOUNT() as "instance",
		c.table_catalog as "database",
        c.table_schema as "schema",
        c.table_name as "table",
        (t.table_type = 'MATERIALIZED VIEW' OR t.table_type = 'VIEW') as "is_view",
        c.column_name as "column",
        c.data_type as "type",
        c.ordinal_position as "position"
	from 
		%[1]s.information_schema.columns as c
	left join 
		%[1]s.information_schema.tables as t on 
		    c.table_catalog = t.table_catalog
			and c.table_name = t.table_name
			and c.table_schema = t.table_schema
	where UPPER(c.table_schema) NOT IN ('INFORMATION_SCHEMA', 'SYSADMIN')
	`

type DbDesc struct {
	Name    string `db:"name" json:"name"`
	Origin  string `db:"origin" json:"origin"`
	Owner   string `db:"owner" json:"owner"`
	Comment string `db:"comment" json:"comment"`
	Kind    string `db:"kind" json:"kind"`
}

func (d *DbDesc) String() string {
	return fmt.Sprintf("Name: %s, Origin: %s, Owner: %s, Comment: %s, Kind: %s", d.Name, d.Origin, d.Owner, d.Comment, d.Kind)
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

func (e *SnowflakeScrapper) QueryCatalog(ctx context.Context) ([]*scrapper.ColumnCatalogRow, error) {
	var results []*scrapper.ColumnCatalogRow

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

		rows, err := e.executor.GetDb().QueryxContext(ctx, fmt.Sprintf(catalogQuery, database))
		if err != nil {
			return nil, err
		}
		defer rows.Close()

		for rows.Next() {
			result := scrapper.ColumnCatalogRow{}
			if err := rows.StructScan(&result); err != nil {
				return nil, err
			}
			result.Instance = e.conf.Account
			results = append(results, &result)
		}

	}

	rows, err := e.executor.GetDb().QueryxContext(ctx, "SHOW SHARES")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	shares := []*ShareDesc{}
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

	return results, nil
}
