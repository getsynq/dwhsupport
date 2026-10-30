package snowflake

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	dwhexecsnowflake "github.com/getsynq/dwhsupport/exec/snowflake"
	"github.com/getsynq/dwhsupport/lazy"
	"github.com/getsynq/dwhsupport/logging"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scope"
	"github.com/getsynq/dwhsupport/sqldialect"
	"github.com/pkg/errors"
	"github.com/samber/lo"
	gosnowflake "github.com/snowflakedb/gosnowflake"
)

func init() {
	gosnowflake.GetLogger().SetLogLevel("fatal")
}

type SnowflakeScrapperConf struct {
	dwhexecsnowflake.SnowflakeConf
	NoGetDll           bool
	AccountUsageDb     *string
	QueryLogsDatabases []string
	// UseAccessHistoryForTableChanges enables ACCESS_HISTORY for FetchTableChangeHistory instead of
	// TABLE_DML_HISTORY. ACCESS_HISTORY provides ~3h lag vs ~6h but requires Snowflake Enterprise edition.
	UseAccessHistoryForTableChanges bool
	// CatalogFromAccountUsage reads databases, schemas, tables, columns and view definitions from
	// ACCOUNT_USAGE in one query for all databases, instead of each database's INFORMATION_SCHEMA.
	// A role with only IMPORTED PRIVILEGES on the SNOWFLAKE database can read ACCOUNT_USAGE, but
	// INFORMATION_SCHEMA and SHOW list only the objects the role has a privilege on. ACCOUNT_USAGE
	// lags behind by up to three hours, so new objects show late. Streams, table constraints,
	// GET_DDL and, without TableMetricsFromAccountUsage, table metrics are read only in the
	// databases SHOW DATABASES lists, and ValidateConfiguration names the others. ACCOUNT_USAGE has
	// no objects of an imported share, so a share is read from its own INFORMATION_SCHEMA, and only
	// with IMPORTED PRIVILEGES on it. With no Databases configured it reads every database, and the
	// scope leaves some out.
	CatalogFromAccountUsage bool
	// TableMetricsFromAccountUsage reads row counts, sizes and last change times from
	// ACCOUNT_USAGE.TABLES in one query for all databases. They lag behind by up to three hours,
	// so a freshness monitor sees a change that much later than through INFORMATION_SCHEMA.
	TableMetricsFromAccountUsage bool
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

var unquotedIdentifier = regexp.MustCompile(`^[A-Z_][A-Z0-9_$]*$`)

// databaseIdent writes a database name as Snowflake reports it, for use as an
// identifier in SQL. A name Snowflake would fold to itself is written as it is,
// and any other one (lower case, a space) is quoted, so it is resolved exactly.
// With CatalogFromAccountUsage and no configured databases, the names come
// from ACCOUNT_USAGE, so any name an account has can get here.
func databaseIdent(name string) string {
	if unquotedIdentifier.MatchString(name) {
		return name
	}
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

func QuoteLiteral(literal string) string {
	// This follows the PostgreSQL internal algorithm for handling quoted literals
	// from libpq, which can be found in the "PQEscapeStringInternal" function,
	// which is found in the libpq/fe-exec.c source file:
	// https://git.postgresql.org/gitweb/?p=postgresql.git;a=blob;f=src/interfaces/libpq/fe-exec.c
	//
	// substitute any single-quotes (') with two single-quotes ('')
	literal = strings.ReplaceAll(literal, `'`, `''`)
	// determine if the string has any backslashes (\) in it.
	// if it does, replace any backslashes (\) with two backslashes (\\)
	// then, we need to wrap the entire string with a PostgreSQL
	// C-style escape. Per how "PQEscapeStringInternal" handles this case, we
	// also add a space before the "E"
	if strings.Contains(literal, `\`) {
		literal = strings.ReplaceAll(literal, `\`, `\\`)
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
	// shownDbs are the databases SHOW DATABASES lists, the ones the role holds a privilege on.
	shownDbs lazy.Lazy[[]*DbDesc]
	executor *dwhexecsnowflake.SnowflakeExecutor
}

func NewSnowflakeScrapper(ctx context.Context, conf *SnowflakeScrapperConf) (*SnowflakeScrapper, error) {
	executor, err := dwhexecsnowflake.NewSnowflakeExecutor(ctx, &conf.SnowflakeConf)
	if err != nil {
		return nil, err
	}
	return newSnowflakeScrapper(ctx, conf, executor), nil
}

func newSnowflakeScrapper(ctx context.Context, conf *SnowflakeScrapperConf, executor *dwhexecsnowflake.SnowflakeExecutor) *SnowflakeScrapper {
	shownDbs := lazy.New(func() ([]*DbDesc, error) {
		return listShownDatabases(ctx, executor)
	})
	existingDbs := shownDbs
	if conf.CatalogFromAccountUsage {
		existingDbs = lazy.New(func() ([]*DbDesc, error) {
			return accountUsageDatabases(ctx, executor, accountUsageDb(conf))
		})
	}
	return &SnowflakeScrapper{conf: conf, executor: executor, existingDbs: existingDbs, shownDbs: shownDbs}
}

func listShownDatabases(ctx context.Context, executor *dwhexecsnowflake.SnowflakeExecutor) ([]*DbDesc, error) {
	rows, err := executor.QueryRows(ctx, "SHOW DATABASES")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var allDatabases []*DbDesc
	for rows.Next() {
		db := &DbDesc{}
		tmp := map[string]any{}
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
}

func (e *SnowflakeScrapper) IsPermissionError(err error) bool {
	return IsPermissionError(err)
}

// IsPermissionError reports whether err is a Snowflake failure that is
// user-actionable and NOT worth retrying. The classification logic is the
// single source of truth in exec/snowflake, shared with the audit/query paths.
func IsPermissionError(err error) bool {
	return dwhexecsnowflake.IsPermissionError(err)
}

func (e *SnowflakeScrapper) Capabilities() scrapper.Capabilities {
	return scrapper.Capabilities{
		EstimateQuery: scrapper.EstimateQueryCapability{Supported: true, ReportsBytes: true},
		PlatformUsers: scrapper.PlatformUsersCapability{Supported: true, Grant: platformUsersGrant},
	}
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

// GetDatabasesToQuery returns the configured databases filtered to only those that
// exist and are accepted by the scope filter. This is the standard way to get the
// list of databases to iterate over in scrapper query methods.
func (e *SnowflakeScrapper) GetDatabasesToQuery(ctx context.Context) ([]string, error) {
	allDatabases, err := e.GetExistingDbs(ctx)
	if err != nil {
		return nil, err
	}

	existingDbs := make(map[string]bool, len(allDatabases))
	for _, database := range allDatabases {
		existingDbs[database.Name] = true
	}

	scopeFilter := scope.GetScope(ctx)

	configured := e.conf.Databases
	// Reading ACCOUNT_USAGE costs the same for every database, so with no
	// database configured it reads all of them, and the scope leaves some out.
	if e.conf.CatalogFromAccountUsage && len(configured) == 0 {
		configured = lo.Map(allDatabases, func(db *DbDesc, _ int) string { return db.Name })
	}

	var result []string
	for _, database := range configured {
		if existingDbs[database] && scopeFilter.IsDatabaseAccepted(database) {
			result = append(result, database)
		}
	}
	return result, nil
}

// DatabasesWithPrivileges returns the databases to query that the role holds
// a privilege on. Only these answer SHOW, GET_DDL and INFORMATION_SCHEMA. It is
// GetDatabasesToQuery unless the catalog is read from ACCOUNT_USAGE, which lists
// databases the role cannot use.
//
// A caller sending its own SHOW or INFORMATION_SCHEMA query to each database
// reads this list, or every database the role cannot use answers with an
// error.
func (e *SnowflakeScrapper) DatabasesWithPrivileges(ctx context.Context) ([]string, error) {
	databases, err := e.GetDatabasesToQuery(ctx)
	if err != nil || !e.conf.CatalogFromAccountUsage {
		return databases, err
	}
	shown, err := e.shownDbs.Get()
	if err != nil {
		return nil, err
	}
	shownNames := lo.SliceToMap(shown, func(db *DbDesc) (string, bool) { return db.Name, true })
	return lo.Filter(databases, func(database string, _ int) bool { return shownNames[database] }), nil
}

// importedShares names the existing databases that are shares imported from
// another account.
func (e *SnowflakeScrapper) importedShares(ctx context.Context) (map[string]bool, error) {
	existing, err := e.GetExistingDbs(ctx)
	if err != nil {
		return nil, err
	}
	shares := map[string]bool{}
	for _, database := range existing {
		if database.Kind == "IMPORTED DATABASE" {
			shares[database.Name] = true
		}
	}
	return shares, nil
}

// splitShares splits the databases to query into the ones the account owns
// and the imported shares the role can use. ACCOUNT_USAGE lists only the
// objects the account owns, so a share is read from its own
// INFORMATION_SCHEMA, which answers only with IMPORTED PRIVILEGES on it. A
// share the role cannot use is in neither list.
func (e *SnowflakeScrapper) splitShares(ctx context.Context) ([]string, []string, error) {
	databases, err := e.GetDatabasesToQuery(ctx)
	if err != nil {
		return nil, nil, err
	}
	shares, err := e.importedShares(ctx)
	if err != nil {
		return nil, nil, err
	}
	withPrivileges, err := e.DatabasesWithPrivileges(ctx)
	if err != nil {
		return nil, nil, err
	}
	usable := lo.SliceToMap(withPrivileges, func(database string) (string, bool) { return database, true })
	var owned, usableShares []string
	for _, database := range databases {
		switch {
		case !shares[database]:
			owned = append(owned, database)
		case usable[database]:
			usableShares = append(usableShares, database)
		}
	}
	return owned, usableShares, nil
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
	var unavailableSharedDbs []string

	var toProbe []string
	for _, database := range e.conf.Databases {
		if !existingDbs[database] {
			missingDbs = append(missingDbs, database)
			continue
		}
		toProbe = append(toProbe, database)
	}
	// A role reading ACCOUNT_USAGE may have no access to the INFORMATION_SCHEMA
	// the probe reads, so it probes only the shares it reads from there.
	if e.conf.CatalogFromAccountUsage {
		if _, toProbe, err = e.splitShares(ctx); err != nil {
			return nil, err
		}
	}
	// A caller accepting incomplete results is told about an unavailable share
	// by every read, as an omission of that database carrying Snowflake's own
	// error, so probing for it here would only say the same thing twice.
	if scrapper.IncompleteResultsAccepted(ctx) {
		toProbe = nil
	}

	for _, database := range toProbe {
		// Probe the database to check if it's an unavailable shared database
		// We use a simple query against information_schema which will fail if the shared database is unavailable
		probeQuery := fmt.Sprintf("SELECT 1 FROM %s.information_schema.tables LIMIT 1", databaseIdent(database))
		probeRows, probeErr := e.executor.QueryRows(ctx, probeQuery)
		if probeRows != nil {
			probeRows.Close()
		}
		if probeErr != nil {
			if isSharedDatabaseUnavailableError(probeErr) {
				logging.GetLogger(ctx).WithField("database", database).WithError(probeErr).
					Warn("Shared database is no longer available")
				unavailableSharedDbs = append(unavailableSharedDbs, database)
			} else {
				// For other errors, log but don't fail validation
				logging.GetLogger(ctx).WithField("database", database).WithError(probeErr).
					Debug("Database probe query failed")
			}
		}
	}

	if len(missingDbs) > 0 {
		warnings = append(warnings, fmt.Sprintf("Database not found or no permissions to access: %s", strings.Join(missingDbs, ", ")))
	}

	if len(unavailableSharedDbs) > 0 {
		warnings = append(
			warnings,
			fmt.Sprintf(
				"Shared database(s) no longer available and will be skipped during data extraction: %s. "+
					"These databases need to be re-created if and when the publisher makes them available again.",
				strings.Join(unavailableSharedDbs, ", "),
			),
		)
	}

	if e.conf.CatalogFromAccountUsage {
		withoutPrivileges, err := e.databasesWithoutPrivileges(ctx)
		if err != nil {
			return nil, err
		}
		shares, err := e.importedShares(ctx)
		if err != nil {
			return nil, err
		}
		unusableShares, withoutPrivileges := lo.FilterReject(withoutPrivileges, func(database string, _ int) bool { return shares[database] })
		if len(withoutPrivileges) > 0 {
			notRead := "their streams, keys and the DDL of their tables"
			if !e.conf.TableMetricsFromAccountUsage {
				notRead = "their table metrics, streams, keys and the DDL of their tables"
			}
			warnings = append(warnings, fmt.Sprintf(
				"The role has no privileges on databases %s. Their tables, columns and view definitions are read from ACCOUNT_USAGE, "+
					"but %s are not. Grant USAGE on these databases and their schemas, "+
					"and REFERENCES on their tables, to read them.",
				strings.Join(withoutPrivileges, ", "), notRead,
			))
		}
		if len(unusableShares) > 0 {
			warnings = append(warnings, fmt.Sprintf(
				"The role cannot use the shared databases %s. ACCOUNT_USAGE does not list the objects of a share, "+
					"so nothing is read from them. Grant IMPORTED PRIVILEGES on them to read them.",
				strings.Join(unusableShares, ", "),
			))
		}
	}

	return warnings, nil
}

// databasesWithoutPrivileges returns the databases to query that the role
// holds no privilege on, so it reads them from ACCOUNT_USAGE only.
func (e *SnowflakeScrapper) databasesWithoutPrivileges(ctx context.Context) ([]string, error) {
	databases, err := e.GetDatabasesToQuery(ctx)
	if err != nil {
		return nil, err
	}
	withPrivileges, err := e.DatabasesWithPrivileges(ctx)
	if err != nil {
		return nil, err
	}
	without, _ := lo.Difference(databases, withPrivileges)
	return without, nil
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

// isSharedDatabaseUnavailableError checks if the error is a Snowflake shared database unavailable error
func isSharedDatabaseUnavailableError(err error) bool {
	if err == nil {
		return false
	}
	var snowflakeErr *gosnowflake.SnowflakeError
	if errors.As(err, &snowflakeErr) {
		// Error code 003030 with SQL state 02000 indicates shared database is unavailable
		return snowflakeErr.Number == 3030 || strings.Contains(snowflakeErr.Message, "Shared database is no longer available")
	}
	return false
}
