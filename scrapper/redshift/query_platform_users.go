package redshift

import (
	"context"
	"strings"
	"time"

	"github.com/getsynq/dwhsupport/exec/querystats"
	dwhexecredshift "github.com/getsynq/dwhsupport/exec/redshift"
	"github.com/getsynq/dwhsupport/logging"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/lib/pq"
	"github.com/pkg/errors"
)

// platformUsersGrant is what lets a role read the facts that are not public:
// the role grants of other users (SVV_USER_GRANTS) and their connections
// (SYS_CONNECTION_LOG). The user list itself (PG_USER) and group membership
// (PG_GROUP) are readable by every user. The sys:monitor role carries it.
const platformUsersGrant = "GRANT ACCESS SYSTEM TABLE TO <user> (or GRANT ROLE sys:monitor TO <user>)"

const platformUsersSource = "redshift.pg_user"

// platformUsersSQL lists every user. PG_USER shows all users to every user, so
// the listing is complete and cannot be refused. The login is usename, the same
// name query history is joined to (query_logs.sql), so an IAM user is listed
// as Redshift spells it, `IAM:jdoe`. rdsdb, the user AWS administers the
// cluster as, is listed like any other.
//
// Disabled is the password expiry (VALID UNTIL) having passed; Redshift has no
// other disabled state, and VALID UNTIL does not stop IAM authentication.
const platformUsersSQL = `
SELECT
	usename::varchar AS login,
	usesysid::varchar AS platform_id,
	(valuntil IS NOT NULL AND valuntil < getdate()) AS disabled
FROM pg_user`

// isSuperuserSQL and hasAccessSystemTableSQL tell whether the connecting user
// sees other users' rows in the SYS and SVV views: a superuser or a holder of
// ACCESS SYSTEM TABLE does, anyone else sees only their own. They are two
// queries because has_system_privilege came with RBAC and an older cluster
// lacks it, which must not hide that a superuser sees everything.
const isSuperuserSQL = `SELECT usesuper AS can_read_others FROM pg_user WHERE usename = current_user`

const hasAccessSystemTableSQL = `SELECT has_system_privilege(current_user, 'ACCESS SYSTEM TABLE') AS can_read_others`

// platformUserGroupsSQL lists group membership. PG_GROUP is readable by every
// user and holds every group's members.
const platformUserGroupsSQL = `
SELECT u.usename::varchar AS login, g.groname::varchar AS role
FROM pg_group g
JOIN pg_user u ON u.usesysid = ANY(g.grolist)`

// platformUserRoleGrantsSQL lists the roles (Redshift RBAC) granted to each
// user directly.
const platformUserRoleGrantsSQL = `
SELECT user_name::varchar AS login, role_name::varchar AS role
FROM svv_user_grants`

// platformUserLastLoginSQL reads the last authenticated connection of each
// user. SYS_CONNECTION_LOG keeps only a few days, so a user who has not
// connected within it has no last login; user_name is CHAR and padded.
const platformUserLastLoginSQL = `
SELECT trim(user_name)::varchar AS login, max(record_time) AS last_login_at
FROM sys_connection_log
WHERE event = 'authenticated'
GROUP BY 1`

type platformUserRow struct {
	Login      string `db:"login"`
	PlatformId string `db:"platform_id"`
	Disabled   bool   `db:"disabled"`
}

type platformUserRoleRow struct {
	Login string `db:"login"`
	Role  string `db:"role"`
}

type platformUserLastLoginRow struct {
	Login       string    `db:"login"`
	LastLoginAt time.Time `db:"last_login_at"`
}

type canReadOthersRow struct {
	CanReadOthers bool `db:"can_read_others"`
}

// skippedPlatformUserFacts are the facts Redshift does not keep about a user.
var skippedPlatformUserFacts = []scrapper.SkippedPlatformUserFact{
	{Fact: scrapper.PlatformUserFactType, Reason: "Redshift has no user type"},
	{Fact: scrapper.PlatformUserFactEmail, Reason: "Redshift keeps no email for a user"},
	{Fact: scrapper.PlatformUserFactDisplayName, Reason: "Redshift keeps no display name for a user"},
	{Fact: scrapper.PlatformUserFactComment, Reason: "Redshift has no comment on a user"},
	{Fact: scrapper.PlatformUserFactCreatedAt, Reason: "Redshift does not record when a user was created"},
	{Fact: scrapper.PlatformUserFactDefaultRole, Reason: "Redshift has no default role"},
}

// QueryPlatformUsers lists every Redshift user with its groups, its directly
// granted roles and its last login. Nothing a grant or the cluster's version
// decides fails the call: a listing that could not be read is returned in its
// refused, unavailable or failed state, and a fact that could not be read
// (SVV_USER_GRANTS before RBAC, a SYS view a provisioned cluster or Serverless
// lacks) is skipped with the reason.
func (e *RedshiftScrapper) QueryPlatformUsers(ctx context.Context) (*scrapper.PlatformUsers, error) {
	collector, ctx := querystats.Start(ctx)
	defer collector.Finish()

	listed, err := scanQuery[platformUserRow](ctx, e, platformUsersSQL, "pg_user")
	if err != nil {
		return scrapper.CollectPlatformUsers(ctx, platformUsersSourceError(err))
	}
	users := &scrapper.PlatformUserListing{
		Source:       platformUsersSource,
		Kind:         scrapper.PlatformUserSourceSQL,
		Completeness: scrapper.PlatformUsersComplete,
	}
	for _, row := range listed {
		disabled := row.Disabled
		users.Users = append(users.Users, &scrapper.PlatformUser{Login: row.Login, PlatformId: row.PlatformId, Disabled: &disabled})
	}
	for _, skipped := range skippedPlatformUserFacts {
		users.Skip(skipped.Fact, skipped.Reason)
	}

	canReadOthers, err := e.canReadOthers(ctx)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		logging.GetLogger(ctx).WithError(err).Warn("failed to check whether the redshift user may read other users' system rows")
	}
	if err := e.addPlatformUserRoles(ctx, users, canReadOthers, err); err != nil {
		return nil, err
	}
	if err := e.addPlatformUserLastLogins(ctx, users, canReadOthers, err); err != nil {
		return nil, err
	}

	collector.SetRowsProduced(int64(len(users.Users)))
	return scrapper.CollectPlatformUsers(ctx, users)
}

// canReadOthers reports whether the connecting user sees every user's rows in
// SVV_USER_GRANTS and SYS_CONNECTION_LOG. An error means it could not tell.
func (e *RedshiftScrapper) canReadOthers(ctx context.Context) (bool, error) {
	super, err := scanQuery[canReadOthersRow](ctx, e, isSuperuserSQL, "pg_user")
	if err != nil {
		return false, err
	}
	if len(super) == 0 {
		return false, errors.New("the connected user is not in pg_user")
	}
	if super[0].CanReadOthers {
		return true, nil
	}
	privilege, err := scanQuery[canReadOthersRow](ctx, e, hasAccessSystemTableSQL, "has_system_privilege")
	if err != nil {
		return false, err
	}
	return len(privilege) > 0 && privilege[0].CanReadOthers, nil
}

// whyNotOthers is the reason a fact that only a user reading other users'
// rows sees in full is not read, or "" when it is to be read.
func whyNotOthers(canReadOthers bool, checkErr error) string {
	switch {
	case checkErr != nil:
		return "could not tell whether the user may read other users' rows (" + platformUserFactSkipReason("has_system_privilege", checkErr) + ")"
	case !canReadOthers:
		return "other users' rows need ACCESS SYSTEM TABLE"
	}
	return ""
}

// addPlatformUserRoles adds group membership, which every user can read, and
// directly granted roles, which only a user that may read other users' rows
// sees in full. Without that privilege the role grants are not read at all,
// so that no user's roles depend on whether it happens to be the connecting
// one, and the fact is reported skipped. It returns an error only when the
// context is done.
func (e *RedshiftScrapper) addPlatformUserRoles(
	ctx context.Context,
	users *scrapper.PlatformUserListing,
	canReadOthers bool,
	checkErr error,
) error {
	groups, err := scanQuery[platformUserRoleRow](ctx, e, platformUserGroupsSQL, "pg_group")
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		logging.GetLogger(ctx).WithError(err).Warn("failed to read redshift group membership")
		users.Skip(scrapper.PlatformUserFactRoles, platformUserFactSkipReason("PG_GROUP", err))
		return nil
	}
	users.AssignRoles(rolesByLogin(groups))

	if why := whyNotOthers(canReadOthers, checkErr); why != "" {
		users.Skip(scrapper.PlatformUserFactRoles, "only group membership was read: role grants (SVV_USER_GRANTS) were not, "+why)
		return nil
	}
	grants, err := scanQuery[platformUserRoleRow](ctx, e, platformUserRoleGrantsSQL, "svv_user_grants")
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		logging.GetLogger(ctx).WithError(err).Warn("failed to read redshift role grants")
		users.Skip(scrapper.PlatformUserFactRoles, "only group membership was read: "+platformUserFactSkipReason("SVV_USER_GRANTS", err))
		return nil
	}
	users.AssignRoles(rolesByLogin(grants))
	return nil
}

// addPlatformUserLastLogins adds each user's last authenticated connection. It
// returns an error only when the context is done.
func (e *RedshiftScrapper) addPlatformUserLastLogins(
	ctx context.Context,
	users *scrapper.PlatformUserListing,
	canReadOthers bool,
	checkErr error,
) error {
	if why := whyNotOthers(canReadOthers, checkErr); why != "" {
		users.Skip(scrapper.PlatformUserFactLastLoginAt, "SYS_CONNECTION_LOG was not read: "+why)
		return nil
	}
	logins, err := scanQuery[platformUserLastLoginRow](ctx, e, platformUserLastLoginSQL, "sys_connection_log")
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		logging.GetLogger(ctx).WithError(err).Warn("failed to read redshift last logins")
		users.Skip(scrapper.PlatformUserFactLastLoginAt, platformUserFactSkipReason("SYS_CONNECTION_LOG", err))
		return nil
	}
	lastLogin := make(map[string]time.Time, len(logins))
	for _, l := range logins {
		lastLogin[l.Login] = l.LastLoginAt
	}
	for _, u := range users.Users {
		if t, ok := lastLogin[u.Login]; ok {
			// record_time is a zone-less UTC timestamp.
			t = time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), time.UTC)
			u.LastLoginAt = &t
		}
	}
	return nil
}

func platformUsersSourceError(err error) *scrapper.PlatformUserListing {
	return scrapper.PlatformUserSourceError(
		platformUsersSource, scrapper.PlatformUserSourceSQL, err, dwhexecredshift.IsPermissionError, isUnavailableError,
	)
}

// isUnavailableError reports whether err says this cluster lacks the view,
// column or function a query names: an older cluster (SVV_USER_GRANTS and
// has_system_privilege came with RBAC), or one of the SYS and SVV views that
// provisioned clusters and Serverless do not share. No grant fixes it.
func isUnavailableError(err error) bool {
	var pqErr *pq.Error
	if !errors.As(err, &pqErr) {
		return false
	}
	switch pqErr.Code {
	case "42P01", // undefined_table
		"42703", // undefined_column
		"42883", // undefined_function
		"0A000": // feature_not_supported
		return true
	}
	return false
}

// platformUserFactSkipReason says why a fact query did not answer, in the
// three states a listing has: refused, unavailable on this cluster, failed.
func platformUserFactSkipReason(source string, err error) string {
	switch {
	case dwhexecredshift.IsPermissionError(err):
		return "reading " + source + " was refused: " + err.Error()
	case isUnavailableError(err):
		return source + " is not available on this cluster: " + err.Error()
	default:
		return "reading " + source + " failed: " + err.Error()
	}
}

func rolesByLogin(memberships []*platformUserRoleRow) map[string][]string {
	roles := map[string][]string{}
	for _, m := range memberships {
		login := strings.TrimSpace(m.Login)
		roles[login] = append(roles[login], strings.TrimSpace(m.Role))
	}
	return roles
}

func scanQuery[T any](ctx context.Context, e *RedshiftScrapper, sql string, source string) ([]*T, error) {
	rows, err := e.executor.QueryRows(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scrapper.ScanAll[T](ctx, rows, source)
}
