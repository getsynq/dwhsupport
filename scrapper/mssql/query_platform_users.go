package mssql

import (
	"context"
	"database/sql"
	_ "embed"
	"time"

	dwhexecmssql "github.com/getsynq/dwhsupport/exec/mssql"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/pkg/errors"
)

//go:embed query_platform_users.sql
var queryPlatformUsersSql string

//go:embed query_platform_user_roles.sql
var queryPlatformUserRolesSql string

// platformUsersGrant is what lets a login see every other login and its roles.
// Without it sys.server_principals shows only the login itself, sa and the
// fixed server roles.
const platformUsersGrant = "VIEW ANY DEFINITION (or ALTER ANY LOGIN) on the server, " +
	"for example GRANT VIEW ANY DEFINITION TO <login>, and VIEW DEFINITION on the database for its role memberships"

// platformUsersVisibilitySql tells whether the login sees every login. Server
// permissions do not exist inside an Azure SQL Database, where both are NULL.
const platformUsersVisibilitySql = `SELECT
	HAS_PERMS_BY_NAME(NULL, NULL, 'VIEW ANY DEFINITION') AS view_any_definition,
	HAS_PERMS_BY_NAME(NULL, NULL, 'ALTER ANY LOGIN') AS alter_any_login`

type platformUserRow struct {
	Login      string         `db:"login"`
	PlatformId sql.NullString `db:"platform_id"`
	Type       sql.NullString `db:"type"`
	Disabled   sql.NullBool   `db:"disabled"`
	CreatedAt  sql.NullTime   `db:"created_at"`
}

type platformUserRoleRow struct {
	Login string `db:"login"`
	Role  string `db:"role"`
}

type platformUsersVisibilityRow struct {
	ViewAnyDefinition sql.NullInt64 `db:"view_any_definition"`
	AlterAnyLogin     sql.NullInt64 `db:"alter_any_login"`
}

// QueryPlatformUsers lists the server's logins (query history and sessions
// report the login name), plus the database's users that authenticate without
// a login. The catalog views are readable by everyone, so the listing is never
// refused; without VIEW ANY DEFINITION it shows only the connecting login and
// sa, and says so.
func (e *MSSQLScrapper) QueryPlatformUsers(ctx context.Context) (*scrapper.PlatformUsers, error) {
	rows, err := selectRows[platformUserRow](ctx, e.executor, queryPlatformUsersSql)
	if err != nil {
		return nil, errors.Wrap(err, "failed to list sys.server_principals")
	}
	result := &scrapper.PlatformUsers{Users: platformUsersFromRows(rows)}
	skipFactsNotOnPlatform(result)

	visibility, err := selectRows[platformUsersVisibilityRow](ctx, e.executor, platformUsersVisibilitySql)
	if err != nil || len(visibility) != 1 {
		result.Completeness = scrapper.PlatformUsersUnknown
		result.CompletenessReason = "could not tell whether the login may see every other login"
	} else {
		setCompleteness(result, visibility[0])
	}

	roles, err := selectRows[platformUserRoleRow](ctx, e.executor, queryPlatformUserRolesSql)
	if err != nil {
		result.Skip(scrapper.PlatformUserFactRoles, factSkipReason(err))
	} else {
		result.AssignRoles(rolesByLogin(roles))
	}
	return result.Finish(), nil
}

func setCompleteness(result *scrapper.PlatformUsers, v *platformUsersVisibilityRow) {
	switch {
	case v.ViewAnyDefinition.Int64 == 1 || v.AlterAnyLogin.Int64 == 1:
		result.Completeness = scrapper.PlatformUsersComplete
	case !v.ViewAnyDefinition.Valid && !v.AlterAnyLogin.Valid:
		result.Completeness = scrapper.PlatformUsersUnknown
		result.CompletenessReason = "the database has no server-level permissions to check (Azure SQL Database); " +
			"its logins are listed in master"
	default:
		result.Completeness = scrapper.PlatformUsersLimited
		result.CompletenessReason = "without VIEW ANY DEFINITION, sys.server_principals shows only the connecting login and sa; grant " +
			platformUsersGrant
	}
}

func platformUsersFromRows(rows []*platformUserRow) []*scrapper.PlatformUser {
	users := make([]*scrapper.PlatformUser, 0, len(rows))
	for _, r := range rows {
		u := &scrapper.PlatformUser{
			Login:      r.Login,
			PlatformId: r.PlatformId.String,
			Type:       r.Type.String,
		}
		if r.Disabled.Valid {
			disabled := r.Disabled.Bool
			u.Disabled = &disabled
		}
		if r.CreatedAt.Valid {
			// The query already shifted it to UTC; the driver labels a
			// datetime with no zone as UTC, so this only fixes the label.
			created := time.Date(r.CreatedAt.Time.Year(), r.CreatedAt.Time.Month(), r.CreatedAt.Time.Day(),
				r.CreatedAt.Time.Hour(), r.CreatedAt.Time.Minute(), r.CreatedAt.Time.Second(), r.CreatedAt.Time.Nanosecond(), time.UTC)
			u.CreatedAt = &created
		}
		users = append(users, u)
	}
	return users
}

func rolesByLogin(rows []*platformUserRoleRow) map[string][]string {
	roles := map[string][]string{}
	for _, r := range rows {
		roles[r.Login] = append(roles[r.Login], r.Role)
	}
	return roles
}

// SQL Server keeps none of these about a login. A login has a default
// database, not a default role.
var factsNotOnPlatform = []scrapper.PlatformUserFact{
	scrapper.PlatformUserFactEmail,
	scrapper.PlatformUserFactDisplayName,
	scrapper.PlatformUserFactComment,
	scrapper.PlatformUserFactLastLoginAt,
	scrapper.PlatformUserFactDefaultRole,
}

func skipFactsNotOnPlatform(result *scrapper.PlatformUsers) {
	for _, f := range factsNotOnPlatform {
		result.Skip(f, "SQL Server does not record it for a login")
	}
}

func factSkipReason(err error) string {
	if dwhexecmssql.IsPermissionError(err) {
		return "the login may not read role memberships; grant " + platformUsersGrant
	}
	return "reading role memberships failed: " + err.Error()
}

func selectRows[T any](ctx context.Context, executor *dwhexecmssql.MSSQLExecutor, query string) ([]*T, error) {
	rows, err := executor.QueryRows(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scrapper.ScanAll[T](ctx, rows, "mssql platform users")
}
