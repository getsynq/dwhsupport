package snowflake

import (
	"context"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/samber/lo"
)

func (e *SnowflakeScrapper) QueryDatabases(ctx context.Context) ([]*scrapper.DatabaseRow, error) {
	dbs, err := e.GetExistingDbs(ctx)
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
			Description:   lo.EmptyableToPtr(db.Comment),
			DatabaseType:  lo.EmptyableToPtr(db.Kind),
			DatabaseOwner: lo.EmptyableToPtr(db.Origin),
		})
	}
	return res, nil
}
