package snowflake

import (
	"context"

	"github.com/getsynq/dwhsupport/scrapper"
)

func (e *SnowflakeScrapper) QueryDatabases(ctx context.Context) ([]*scrapper.DatabaseRow, error) {
	dbs, err := e.allAllowedDatabases(ctx)
	if err != nil {
		return nil, err
	}
	var res []*scrapper.DatabaseRow
	for _, db := range dbs {
		if ignoreShare(db.Origin) {
			continue
		}
		res = append(res, &scrapper.DatabaseRow{
			Instance:      e.conf.Account,
			Database:      db.Name,
			Description:   db.Comment,
			DatabaseType:  db.Kind,
			DatabaseOwner: db.Origin,
		})
	}
	return res, nil
}
