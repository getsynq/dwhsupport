package clickhouse

import (
	"context"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/getsynq/dwhsupport/logging"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/pkg/errors"
)

// platformUsersGrant is what a ClickHouse user needs to see every user and every
// fact. system.users and system.role_grants are refused outright without their
// grant (the connecting user cannot even list itself), and the cluster fan-out
// every system table read goes through needs READ ON REMOTE unless the scrapper
// is configured for a single node.
const platformUsersGrant = "GRANT SELECT ON system.users (or SHOW USERS) to list users, " +
	"SELECT ON system.role_grants for their roles, SELECT ON system.session_log for their last login, " +
	"and READ ON REMOTE for the cluster-wide read unless the connection is single-node"

// platformUsersSql reads every user. A user stored in users.xml exists only on
// the node whose config holds it, so the read fans out like every system table
// read and folds the replicas' copies into one row per name.
//
// A user is disabled when every way it can authenticate has expired:
// valid_until is one entry per authentication method, the epoch meaning no
// expiry. ClickHouse has no other way to disable a user.
const platformUsersSql = `
SELECT
    name AS login,
    toString(any(id)) AS platform_id,
    max(length(valid_until) > 0 AND arrayAll(v -> v != toDateTime(0) AND v < now(), valid_until)) AS disabled
FROM clusterAllReplicas(default, system.users)
GROUP BY name
`

// platformUsersNoValidUntilSql is for a server older than valid_until (added in
// 23.9), where an expired user cannot be told apart from an active one.
const platformUsersNoValidUntilSql = `
SELECT
    name AS login,
    toString(any(id)) AS platform_id
FROM clusterAllReplicas(default, system.users)
GROUP BY name
`

// platformUserRolesSql reads the roles granted directly to each user. A role
// granted to a role is not a user's role and is not read here.
const platformUserRolesSql = `
SELECT DISTINCT
    user_name,
    granted_role_name,
    granted_role_is_default
FROM clusterAllReplicas(default, system.role_grants)
WHERE user_name IS NOT NULL
`

// platformUserLastLoginSql reads each user's last successful login from the
// session log, which a server keeps only when it is configured to.
const platformUserLastLoginSql = `
SELECT
    user,
    max(event_time) AS last_login_at
FROM clusterAllReplicas(default, system.session_log)
WHERE type = 'LoginSuccess'
GROUP BY user
`

type platformUserRow struct {
	Login      string `db:"login"`
	PlatformId string `db:"platform_id"`
	Disabled   *uint8 `db:"disabled"`
}

type platformUserRoleRow struct {
	UserName  *string `db:"user_name"`
	Role      string  `db:"granted_role_name"`
	IsDefault uint8   `db:"granted_role_is_default"`
}

type platformUserLastLoginRow struct {
	User        string    `db:"user"`
	LastLoginAt time.Time `db:"last_login_at"`
}

// ClickHouse error codes the user listing tells apart from a failure.
const (
	chErrUnknownIdentifier = 47
	chErrUnknownTable      = 60
)

func isClickhouseErrCode(err error, code int32) bool {
	var ex *clickhouse.Exception
	return errors.As(err, &ex) && ex.Code == code
}

// QueryPlatformUsers lists system.users with the roles granted to each user
// (system.role_grants) and, where the server keeps a session log, their last
// login. ClickHouse states no user type, email, display name, comment or
// creation time, so those are always skipped. A refused system.users is a
// permission error; a refused role or session log read only skips that fact.
//
// The listing is complete: system.users is either refused or lists every user,
// there is no partial view of it.
func (e *ClickhouseScrapper) QueryPlatformUsers(ctx context.Context) (*scrapper.PlatformUsers, error) {
	result := &scrapper.PlatformUserListing{
		Source:       "clickhouse.system.users",
		Kind:         scrapper.PlatformUserSourceSQL,
		Completeness: scrapper.PlatformUsersComplete,
	}

	rows, err := e.readPlatformUsers(ctx, platformUsersSql)
	if isClickhouseErrCode(err, chErrUnknownIdentifier) {
		rows, err = e.readPlatformUsers(ctx, platformUsersNoValidUntilSql)
		result.Skip(scrapper.PlatformUserFactDisabled, "this ClickHouse version has no system.users.valid_until")
	}
	if err != nil {
		return nil, errors.Wrap(err, "failed to list clickhouse users from system.users")
	}
	for _, r := range rows {
		u := &scrapper.PlatformUser{Login: r.Login, PlatformId: r.PlatformId}
		if r.Disabled != nil {
			disabled := *r.Disabled != 0
			u.Disabled = &disabled
		}
		result.Users = append(result.Users, u)
	}

	for _, f := range []scrapper.PlatformUserFact{
		scrapper.PlatformUserFactType,
		scrapper.PlatformUserFactEmail,
		scrapper.PlatformUserFactDisplayName,
		scrapper.PlatformUserFactComment,
		scrapper.PlatformUserFactCreatedAt,
	} {
		result.Skip(f, "ClickHouse does not record it for a user")
	}

	e.addPlatformUserRoles(ctx, result)
	e.addPlatformUserLastLogin(ctx, result)

	return scrapper.NewPlatformUsers(result), nil
}

func (e *ClickhouseScrapper) readPlatformUsers(ctx context.Context, sql string) ([]*platformUserRow, error) {
	rows, err := e.executor.QueryRows(ctx, e.systemTablesSql(sql))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scrapper.ScanAll[platformUserRow](ctx, rows, "system.users")
}

// addPlatformUserRoles adds each user's directly granted roles, and the default
// role when exactly one of them is a default: DefaultRole is one role, and
// ClickHouse may start a session with several.
func (e *ClickhouseScrapper) addPlatformUserRoles(ctx context.Context, result *scrapper.PlatformUserListing) {
	grants, err := e.readRoleGrants(ctx)
	if err != nil {
		reason := skipReason(e, err, "SELECT ON system.role_grants")
		logging.GetLogger(ctx).WithError(err).Warn("failed to read clickhouse role grants, listing users without their roles")
		result.Skip(scrapper.PlatformUserFactRoles, reason)
		result.Skip(scrapper.PlatformUserFactDefaultRole, reason)
		return
	}

	roles := map[string][]string{}
	defaults := map[string][]string{}
	for _, g := range grants {
		if g.UserName == nil {
			continue
		}
		roles[*g.UserName] = append(roles[*g.UserName], g.Role)
		if g.IsDefault != 0 {
			defaults[*g.UserName] = append(defaults[*g.UserName], g.Role)
		}
	}
	result.AssignRoles(roles)
	for _, u := range result.Users {
		if d := defaults[u.Login]; len(d) == 1 {
			u.DefaultRole = d[0]
		}
	}
}

func (e *ClickhouseScrapper) readRoleGrants(ctx context.Context) ([]*platformUserRoleRow, error) {
	rows, err := e.executor.QueryRows(ctx, e.systemTablesSql(platformUserRolesSql))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scrapper.ScanAll[platformUserRoleRow](ctx, rows, "system.role_grants")
}

func (e *ClickhouseScrapper) addPlatformUserLastLogin(ctx context.Context, result *scrapper.PlatformUserListing) {
	logins, err := e.readLastLogins(ctx)
	if err != nil {
		var reason string
		if isClickhouseErrCode(err, chErrUnknownTable) {
			reason = "the server keeps no system.session_log (session_log is not configured)"
		} else {
			reason = skipReason(e, err, "SELECT ON system.session_log")
			logging.GetLogger(ctx).WithError(err).Warn("failed to read clickhouse session log, listing users without their last login")
		}
		result.Skip(scrapper.PlatformUserFactLastLoginAt, reason)
		return
	}

	lastLogin := make(map[string]time.Time, len(logins))
	for _, l := range logins {
		lastLogin[l.User] = l.LastLoginAt
	}
	for _, u := range result.Users {
		if t, ok := lastLogin[u.Login]; ok && !t.IsZero() {
			t = t.UTC()
			u.LastLoginAt = &t
		}
	}
}

func (e *ClickhouseScrapper) readLastLogins(ctx context.Context) ([]*platformUserLastLoginRow, error) {
	rows, err := e.executor.QueryRows(ctx, e.systemTablesSql(platformUserLastLoginSql))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scrapper.ScanAll[platformUserLastLoginRow](ctx, rows, "system.session_log")
}

// skipReason says why a fact query failed: the grant it needs when it was
// refused, the error otherwise.
func skipReason(e *ClickhouseScrapper, err error, grant string) string {
	if e.IsPermissionError(err) {
		return "refused, needs " + grant
	}
	msg := err.Error()
	if i := strings.IndexByte(msg, '\n'); i >= 0 {
		msg = msg[:i]
	}
	return "failed: " + msg
}
