package clickhouse

import (
	"context"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/getsynq/dwhsupport/logging"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

// platformUsersSource names the one source of ClickHouse users.
const platformUsersSource = "clickhouse.system.users"

// platformUsersGrant is what a ClickHouse user needs to see every user and every
// fact. system.users and system.role_grants are refused outright without their
// grant (the connecting user cannot even list itself), and the cluster fan-out
// every system table read goes through needs READ ON REMOTE unless the scrapper
// is configured for a single node.
const platformUsersGrant = "GRANT SELECT ON system.users (or SHOW USERS) to list users, " +
	"SELECT ON system.role_grants for their roles, SELECT ON system.session_log for their last login, " +
	"and READ ON REMOTE for the cluster-wide read unless the connection is single-node"

// platformUsersQuery is one way of reading system.users. A user stored in
// users.xml exists only on the node whose config holds it, so every variant fans
// out like every system table read and folds the replicas' copies into one row
// per name.
type platformUsersQuery struct {
	sql string
	// noDisabled says the variant cannot tell an expired user from an active one.
	noDisabled bool
}

// platformUsersQueries are tried in order until one runs, newest server first.
// A user is disabled when every way it can authenticate has expired; ClickHouse
// has no other way to disable a user. valid_until became one entry per
// authentication method (the epoch meaning no expiry) when a user could have
// several; before that it was a single nullable value, and before 23.9 it did
// not exist.
var platformUsersQueries = []platformUsersQuery{
	{sql: `
SELECT
    name AS login,
    toString(any(id)) AS platform_id,
    max(length(valid_until) > 0 AND arrayAll(v -> v != toDateTime(0) AND v < now(), valid_until)) AS disabled
FROM clusterAllReplicas(default, system.users)
GROUP BY name
`},
	{sql: `
SELECT
    name AS login,
    toString(any(id)) AS platform_id,
    max(ifNull(valid_until != toDateTime(0) AND valid_until < now(), 0)) AS disabled
FROM clusterAllReplicas(default, system.users)
GROUP BY name
`},
	{noDisabled: true, sql: `
SELECT
    name AS login,
    toString(any(id)) AS platform_id
FROM clusterAllReplicas(default, system.users)
GROUP BY name
`},
}

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
	chErrNoSuchColumnInTable   = 16
	chErrIllegalTypeOfArgument = 43
	chErrUnknownFunction       = 46
	chErrUnknownIdentifier     = 47
	chErrUnknownTable          = 60
	chErrBadGet                = 170 // "Requested cluster ... not found" before CLUSTER_DOESNT_EXIST
	chErrClusterDoesntExist    = 701
)

func clickhouseErrCode(err error) (int32, bool) {
	var ex *clickhouse.Exception
	if errors.As(err, &ex) {
		return ex.Code, true
	}
	return 0, false
}

func isClickhouseErrCode(err error, codes ...int32) bool {
	code, ok := clickhouseErrCode(err)
	if !ok {
		return false
	}
	for _, c := range codes {
		if code == c {
			return true
		}
	}
	return false
}

// isUnavailableOnThisServer reports an error that says this server's version or
// edition lacks a table, column or function the query reads. No grant fixes it.
func isUnavailableOnThisServer(err error) bool {
	return isClickhouseErrCode(err, chErrNoSuchColumnInTable, chErrIllegalTypeOfArgument, chErrUnknownFunction,
		chErrUnknownIdentifier, chErrUnknownTable)
}

// systemReadScope says what part of the service a system table read saw.
type systemReadScope int

const (
	// readCluster: the read went through the configured fan-out (or the
	// connection is configured single-node).
	readCluster systemReadScope = iota
	// readLocalRemoteRefused: the fan-out needs READ ON REMOTE, which the role
	// lacks, so the connected node was read alone.
	readLocalRemoteRefused
	// readLocalNoCluster: the configured cluster is not defined on this server,
	// so the connected node was read alone.
	readLocalNoCluster
)

// fanOutFallback says whether a fan-out read that failed with err should be
// retried on the connected node alone, and why.
func (e *ClickhouseScrapper) fanOutFallback(err error) (systemReadScope, bool) {
	if e.conf.Cluster.SingleNode {
		return readCluster, false
	}
	if isClickhouseErrCode(err, chErrClusterDoesntExist, chErrBadGet) {
		return readLocalNoCluster, true
	}
	if e.IsPermissionError(err) && strings.Contains(err.Error(), "REMOTE") {
		return readLocalRemoteRefused, true
	}
	return readCluster, false
}

// querySystem runs a system table read through the cluster fan-out and, when
// the fan-out itself is what failed (no READ ON REMOTE, no such cluster), again
// on the connected node alone. scan reads the result set before it is closed.
func (e *ClickhouseScrapper) querySystem(ctx context.Context, sql string, scan func(*sqlx.Rows) error) (systemReadScope, error) {
	err := e.querySystemAs(ctx, e.systemTablesSql(sql), scan)
	if err == nil {
		return readCluster, nil
	}
	scope, ok := e.fanOutFallback(err)
	if !ok || ctx.Err() != nil {
		return readCluster, err
	}
	local := ClusterConf{SingleNode: true}.resolveSystemTables(sql)
	return scope, e.querySystemAs(ctx, local, scan)
}

func (e *ClickhouseScrapper) querySystemAs(ctx context.Context, sql string, scan func(*sqlx.Rows) error) error {
	rows, err := e.executor.QueryRows(ctx, sql)
	if err != nil {
		return err
	}
	defer rows.Close()
	return scan(rows)
}

// QueryPlatformUsers lists system.users with the roles granted to each user
// (system.role_grants) and, where the server keeps a session log, their last
// login. ClickHouse states no user type, email, display name, comment or
// creation time, so those are always skipped.
//
// Nothing a grant or the server version decides fails the call: a refused
// system.users is a refused source, one this server lacks an unavailable one,
// and a refused or missing role or session log read only skips that fact. When
// the cluster fan-out is refused (no READ ON REMOTE) or the cluster is not
// defined, the connected node is read alone and the listing says so.
//
// Otherwise the listing is complete: system.users is either refused or lists
// every user, there is no partial view of it.
func (e *ClickhouseScrapper) QueryPlatformUsers(ctx context.Context) (*scrapper.PlatformUsers, error) {
	result := &scrapper.PlatformUserListing{
		Source:       platformUsersSource,
		Kind:         scrapper.PlatformUserSourceSQL,
		Completeness: scrapper.PlatformUsersComplete,
	}

	rows, scope, query, err := e.readPlatformUsers(ctx)
	if err != nil {
		return scrapper.CollectPlatformUsers(ctx,
			scrapper.PlatformUserSourceError(platformUsersSource, scrapper.PlatformUserSourceSQL,
				errors.Wrap(err, "failed to list clickhouse users from system.users"), e.IsPermissionError, isUnavailableOnThisServer))
	}
	switch scope {
	case readLocalRemoteRefused:
		result.Completeness = scrapper.PlatformUsersLimited
		result.CompletenessReason = "read on the connected node only, the cluster-wide read needs READ ON REMOTE: " +
			"a user defined in another node's users.xml is not listed"
	case readLocalNoCluster:
		result.Completeness = scrapper.PlatformUsersUnknown
		result.CompletenessReason = "the cluster " + e.conf.Cluster.clusterName() + " is not defined on this server, " +
			"read on the connected node only: complete for a single-node server, not for a cluster under another name"
	}
	if query.noDisabled {
		result.Skip(scrapper.PlatformUserFactDisabled, "this ClickHouse version has no system.users.valid_until")
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

	return scrapper.CollectPlatformUsers(ctx, result)
}

// readPlatformUsers tries each platformUsersQueries variant until one runs. A
// refusal ends it, since an older variant reads the same table; so does a
// failure that is not about this server lacking what the variant reads.
func (e *ClickhouseScrapper) readPlatformUsers(ctx context.Context) ([]*platformUserRow, systemReadScope, platformUsersQuery, error) {
	var err error
	for _, q := range platformUsersQueries {
		var rows []*platformUserRow
		var scope systemReadScope
		scope, err = e.querySystem(ctx, q.sql, func(r *sqlx.Rows) error {
			var scanErr error
			rows, scanErr = scrapper.ScanAll[platformUserRow](ctx, r, "system.users")
			return scanErr
		})
		if err == nil {
			return rows, scope, q, nil
		}
		if ctx.Err() != nil || !isUnavailableOnThisServer(err) {
			return nil, scope, q, err
		}
	}
	return nil, readCluster, platformUsersQuery{}, err
}

// addPlatformUserRoles adds each user's directly granted roles, and the default
// role when exactly one of them is a default: DefaultRole is one role, and
// ClickHouse may start a session with several.
func (e *ClickhouseScrapper) addPlatformUserRoles(ctx context.Context, result *scrapper.PlatformUserListing) {
	var grants []*platformUserRoleRow
	_, err := e.querySystem(ctx, platformUserRolesSql, func(r *sqlx.Rows) error {
		var scanErr error
		grants, scanErr = scrapper.ScanAll[platformUserRoleRow](ctx, r, "system.role_grants")
		return scanErr
	})
	if err != nil {
		reason := e.factSkipReason(err, "SELECT ON system.role_grants")
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

func (e *ClickhouseScrapper) addPlatformUserLastLogin(ctx context.Context, result *scrapper.PlatformUserListing) {
	var logins []*platformUserLastLoginRow
	_, err := e.querySystem(ctx, platformUserLastLoginSql, func(r *sqlx.Rows) error {
		var scanErr error
		logins, scanErr = scrapper.ScanAll[platformUserLastLoginRow](ctx, r, "system.session_log")
		return scanErr
	})
	if err != nil {
		reason := e.factSkipReason(err, "SELECT ON system.session_log")
		if isClickhouseErrCode(err, chErrUnknownTable) {
			reason = "the server keeps no system.session_log (session_log is not configured)"
		} else {
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

// factSkipReason says why a fact query failed: the grant it needs when it was
// refused, that this server lacks it, or the error otherwise.
func (e *ClickhouseScrapper) factSkipReason(err error, grant string) string {
	msg := err.Error()
	if i := strings.IndexByte(msg, '\n'); i >= 0 {
		msg = msg[:i]
	}
	switch {
	case e.IsPermissionError(err):
		return "refused, needs " + grant
	case isUnavailableOnThisServer(err):
		return "unavailable on this ClickHouse version: " + msg
	default:
		return "failed: " + msg
	}
}
