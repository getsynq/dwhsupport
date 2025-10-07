package snowflake

import (
	"context"
	"fmt"
	"strings"

	dwhexecsnowflake "github.com/getsynq/dwhsupport/exec/snowflake"
	"github.com/getsynq/dwhsupport/lazy"
	"github.com/getsynq/dwhsupport/logging"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/sqldialect"
	"github.com/sirupsen/logrus"
	gosnowflake "github.com/snowflakedb/gosnowflake"
)

type formatSetter interface {
	SetFormatter(formatter logrus.Formatter)
}

func init() {
	gosnowflake.GetLogger().SetLogLevel("fatal")
}

type SnowflakeScrapperConf struct {
	dwhexecsnowflake.SnowflakeConf
	NoGetDll       bool
	AccountUsageDb *string
}

// FIXME: I couldn't make it work with `foo IN (?)` binding, so I'm using this
func (c *SnowflakeScrapperConf) UpperDatabasesLiteral() string {
	if len(c.Databases) == 0 {
		return ""
	}
	parts := make([]string, len(c.Databases))
	for i, db := range c.Databases {
		parts[i] = QuoteLiteral(strings.ToUpper(db))
	}
	return strings.Join(parts, ", ")
}

func QuoteLiteral(literal string) string {
	// This follows the PostgreSQL internal algorithm for handling quoted literals
	// from libpq, which can be found in the "PQEscapeStringInternal" function,
	// which is found in the libpq/fe-exec.c source file:
	// https://git.postgresql.org/gitweb/?p=postgresql.git;a=blob;f=src/interfaces/libpq/fe-exec.c
	//
	// substitute any single-quotes (') with two single-quotes ('')
	literal = strings.Replace(literal, `'`, `''`, -1)
	// determine if the string has any backslashes (\) in it.
	// if it does, replace any backslashes (\) with two backslashes (\\)
	// then, we need to wrap the entire string with a PostgreSQL
	// C-style escape. Per how "PQEscapeStringInternal" handles this case, we
	// also add a space before the "E"
	if strings.Contains(literal, `\`) {
		literal = strings.Replace(literal, `\`, `\\`, -1)
		literal = ` E'` + literal + `'`
	} else {
		// otherwise, we can just wrap the literal with a pair of single quotes
		literal = `'` + literal + `'`
	}
	return literal
}

var _ scrapper.Scrapper = &SnowflakeScrapper{}

type SnowflakeScrapper struct {
	conf        *SnowflakeScrapperConf
	existingDbs lazy.Lazy[[]*DbDesc]
	executor    *dwhexecsnowflake.SnowflakeExecutor
}

func NewSnowflakeScrapper(ctx context.Context, conf *SnowflakeScrapperConf) (*SnowflakeScrapper, error) {
	executor, err := dwhexecsnowflake.NewSnowflakeExecutor(ctx, &conf.SnowflakeConf)
	if err != nil {
		return nil, err
	}

	lazyExistingDbs := lazy.New[[]*DbDesc](func() ([]*DbDesc, error) {
		rows, err := executor.GetDb().QueryxContext(ctx, fmt.Sprintf("SHOW DATABASES"))
		if err != nil {
			return nil, err
		}
		defer rows.Close()

		var allDatabases []*DbDesc
		for rows.Next() {
			db := &DbDesc{}
			tmp := map[string]interface{}{}
			if err := rows.MapScan(tmp); err != nil {
				return nil, err
			}
			db.Name = tmp["name"].(string)
			db.Origin = tmp["origin"].(string)
			db.Owner = tmp["owner"].(string)
			db.Comment = tmp["comment"].(string)
			db.Kind = tmp["kind"].(string)
			allDatabases = append(allDatabases, db)
		}
		logging.GetLogger(ctx).WithField("all_databases", allDatabases).Info("show databases")

		return allDatabases, nil
	})

	return &SnowflakeScrapper{conf: conf, executor: executor, existingDbs: lazyExistingDbs}, nil
}

func (e *SnowflakeScrapper) IsPermissionError(err error) bool {
	return false
}

func (e *SnowflakeScrapper) DialectType() string {
	return "snowflake"
}

func (e *SnowflakeScrapper) SqlDialect() sqldialect.Dialect {
	return sqldialect.NewSnowflakeDialect()
}

func (e *SnowflakeScrapper) GetExistingDbs(ctx context.Context) ([]*DbDesc, error) {
	return e.existingDbs.Get()
}

func (e *SnowflakeScrapper) ValidateConfiguration(ctx context.Context) ([]string, error) {
	var warnings []string

	allDatabases, err := e.GetExistingDbs(ctx)
	if err != nil {
		return nil, err
	}

	existingDbs := map[string]bool{}
	for _, database := range allDatabases {
		existingDbs[database.Name] = true
	}
	var missingDbs []string
	for _, database := range e.conf.Databases {
		if !existingDbs[database] {
			missingDbs = append(missingDbs, database)
		}
	}

	if len(missingDbs) > 0 {
		warnings = append(warnings, fmt.Sprintf("Database not found or no permissions to access %s", strings.Join(missingDbs, ", ")))
	}

	return warnings, nil
}

func (e *SnowflakeScrapper) Close() error {
	return e.executor.Close()
}

type DbDesc struct {
	Name    string `db:"name"    json:"name"`
	Origin  string `db:"origin"  json:"origin"`
	Owner   string `db:"owner"   json:"owner"`
	Comment string `db:"comment" json:"comment"`
	Kind    string `db:"kind"    json:"kind"`
}

func (d *DbDesc) String() string {
	return fmt.Sprintf("Name: %s, Origin: %s, Owner: %s, Comment: %s, Kind: %s", d.Name, d.Origin, d.Owner, d.Comment, d.Kind)
}

func (e *SnowflakeScrapper) Executor() *dwhexecsnowflake.SnowflakeExecutor {
	return e.executor
}

func ignoreShare(ownerAccount string) bool {
	if ownerAccount == "SNOWFLAKE" {
		return true
	}
	if strings.HasPrefix(ownerAccount, "SFSALESSHARED.") {
		return true
	}
	if strings.HasPrefix(ownerAccount, "WEATHERSOURCE.") {
		return true
	}
	if strings.HasPrefix(ownerAccount, "KNOEMA.") {
		return true
	}
	return false
}
