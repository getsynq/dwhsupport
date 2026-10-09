package oracle

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"

	dwhexecoracle "github.com/getsynq/dwhsupport/exec/oracle"
	"github.com/getsynq/dwhsupport/logging"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

// platformUsersGrant is what lets a role read DBA_USERS and DBA_ROLE_PRIVS.
const platformUsersGrant = "SELECT_CATALOG_ROLE (or SELECT ANY DICTIONARY), for DBA_USERS and DBA_ROLE_PRIVS"

// Source names of the Oracle user listings.
const (
	oracleDbaUsersSource = "oracle.dba_users"
	oracleAllUsersSource = "oracle.all_users"
)

// platformUserViews names the dictionary views the listing reads. The tests
// point one at a view that does not exist: Oracle answers a refused view and a
// missing one with the same ORA-00942, so that is exactly what a role without
// the grant sees.
type platformUserViews struct {
	users     string
	roleGrant string
}

var defaultPlatformUserViews = platformUserViews{users: "DBA_USERS", roleGrant: "DBA_ROLE_PRIVS"}

// Oracle keeps a DATE in the database server's clock with no zone, and
// SYSTIMESTAMP carries that clock's offset, so CREATED is read at the offset
// the server runs at now. LAST_LOGIN is a TIMESTAMP WITH TIME ZONE.
const dbaUsersSql = `SELECT
	username,
	user_id,
	account_status,
	authentication_type,
	SYS_EXTRACT_UTC(FROM_TZ(CAST(created AS TIMESTAMP), TO_CHAR(SYSTIMESTAMP, 'TZH:TZM'))) AS created_utc,
	SYS_EXTRACT_UTC(last_login) AS last_login_utc
FROM %s`

// dbaUsersBaseSql keeps the columns every Oracle version has. LAST_LOGIN came
// in 12c and AUTHENTICATION_TYPE in 11g; an older DBA_USERS answers dbaUsersSql
// with ORA-00904.
const dbaUsersBaseSql = `SELECT
	username,
	user_id,
	account_status,
	SYS_EXTRACT_UTC(FROM_TZ(CAST(created AS TIMESTAMP), TO_CHAR(SYSTIMESTAMP, 'TZH:TZM'))) AS created_utc
FROM %s`

const allUsersSql = `SELECT
	username,
	user_id,
	SYS_EXTRACT_UTC(FROM_TZ(CAST(created AS TIMESTAMP), TO_CHAR(SYSTIMESTAMP, 'TZH:TZM'))) AS created_utc
FROM ALL_USERS`

const roleGrantsSql = `SELECT grantee, granted_role FROM %s`

type oracleUserRow struct {
	Username           string         `db:"USERNAME"`
	UserId             sql.NullInt64  `db:"USER_ID"`
	AccountStatus      sql.NullString `db:"ACCOUNT_STATUS"`
	AuthenticationType sql.NullString `db:"AUTHENTICATION_TYPE"`
	CreatedUtc         sql.NullTime   `db:"CREATED_UTC"`
	LastLoginUtc       sql.NullTime   `db:"LAST_LOGIN_UTC"`
}

type oracleRoleGrantRow struct {
	Grantee     string `db:"GRANTEE"`
	GrantedRole string `db:"GRANTED_ROLE"`
}

// rowQuerier is the part of the executor the listing needs, so the tests can
// answer it with sqlmock.
type rowQuerier interface {
	QueryRows(ctx context.Context, sql string, args ...interface{}) (*sqlx.Rows, error)
}

// isUnavailable reports an error that says this Oracle version has no such
// column (ORA-00904 invalid identifier) or object (ORA-04043). ORA-00942
// "table or view does not exist" is not one of them: Oracle answers a view the
// role may not read with it too, and a grant usually fixes it, so it is a
// refusal (IsPermissionError).
func isUnavailable(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "ORA-00904") || strings.Contains(msg, "ORA-04043")
}

// QueryPlatformUsers lists the database users from DBA_USERS. Login is
// USERNAME, the name V$SQL reports as PARSING_SCHEMA_NAME in query logs; Type
// is AUTHENTICATION_TYPE (PASSWORD, EXTERNAL, GLOBAL, NONE). Oracle-maintained
// users (SYS, SYSTEM, ...) are listed too: they are logins, and SYSTEM in
// particular runs queries. Roles come from DBA_ROLE_PRIVS.
//
// ALL_USERS lists the same users with only their name, id and creation time,
// so it is read only when DBA_USERS did not answer, whatever the reason: a
// refusal, a version without the view, or a failure of that one statement. A
// connection that is down fails ALL_USERS too, and the call then fails; one
// that is up still returns every user.
func (e *OracleScrapper) QueryPlatformUsers(ctx context.Context) (*scrapper.PlatformUsers, error) {
	return listOraclePlatformUsers(ctx, e.executor, defaultPlatformUserViews)
}

func listOraclePlatformUsers(ctx context.Context, q rowQuerier, views platformUserViews) (*scrapper.PlatformUsers, error) {
	listing := &scrapper.PlatformUserListing{
		Source:       oracleDbaUsersSource,
		Kind:         scrapper.PlatformUserSourceSQL,
		Completeness: scrapper.PlatformUsersComplete,
	}

	rows, err := selectOracleUsers(ctx, q, fmt.Sprintf(dbaUsersSql, views.users), views.users)
	if err != nil && isUnavailable(err) && ctx.Err() == nil {
		logging.GetLogger(ctx).
			WithError(err).
			Warnf("%s lacks a column of this Oracle version, reading it without LAST_LOGIN and AUTHENTICATION_TYPE", views.users)
		rows, err = selectOracleUsers(ctx, q, fmt.Sprintf(dbaUsersBaseSql, views.users), views.users)
		if err == nil {
			reason := "this Oracle version's DBA_USERS has no LAST_LOGIN or AUTHENTICATION_TYPE column (both need 12c)"
			listing.Skip(scrapper.PlatformUserFactType, scrapper.PlatformUserSkipUnavailable, reason)
			listing.Skip(scrapper.PlatformUserFactLastLoginAt, scrapper.PlatformUserSkipUnavailable, reason)
		}
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, ctxErr
	}
	var notAnswered *scrapper.PlatformUserListing
	if err != nil {
		logging.GetLogger(ctx).WithError(err).Warnf("cannot read %s, listing users from ALL_USERS", views.users)
		notAnswered = scrapper.PlatformUserSourceError(
			oracleDbaUsersSource, scrapper.PlatformUserSourceSQL, err, dwhexecoracle.IsPermissionError, isUnavailable,
		)
		listing.Source = oracleAllUsersSource
		// The facts only DBA_USERS has are skipped the way DBA_USERS did not
		// answer: refused when a grant would let it.
		dbaUsersKind := scrapper.PlatformUserSkipKindOf(err, dwhexecoracle.IsPermissionError, isUnavailable)
		rows, err = selectOracleUsers(ctx, q, allUsersSql, "ALL_USERS")
		if err != nil {
			return scrapper.CollectPlatformUsers(ctx, notAnswered, scrapper.PlatformUserSourceError(
				oracleAllUsersSource, scrapper.PlatformUserSourceSQL, err, dwhexecoracle.IsPermissionError, isUnavailable,
			))
		}
		reason := "ALL_USERS has no such column; DBA_USERS has it (" + platformUsersGrant + ")"
		listing.Skip(scrapper.PlatformUserFactType, dbaUsersKind, reason)
		listing.Skip(scrapper.PlatformUserFactDisabled, dbaUsersKind, reason)
		listing.Skip(scrapper.PlatformUserFactLastLoginAt, dbaUsersKind, reason)
	}
	listing.Skip(scrapper.PlatformUserFactEmail, scrapper.PlatformUserSkipUnavailable, "Oracle keeps no email for a database user")
	listing.Skip(scrapper.PlatformUserFactDisplayName, scrapper.PlatformUserSkipUnavailable, "Oracle keeps no display name for a database user")
	listing.Skip(scrapper.PlatformUserFactComment, scrapper.PlatformUserSkipUnavailable, "Oracle keeps no comment on a database user")
	listing.Skip(
		scrapper.PlatformUserFactDefaultRole,
		scrapper.PlatformUserSkipUnavailable,
		"Oracle enables every default role of a user at once, so there is no single default role; see roles",
	)

	for _, row := range rows {
		listing.Users = append(listing.Users, row.toPlatformUser())
	}

	grants, err := selectRoleGrants(ctx, q, views.roleGrant)
	switch {
	case err == nil:
		rolesByLogin := map[string][]string{}
		for _, g := range grants {
			rolesByLogin[g.Grantee] = append(rolesByLogin[g.Grantee], g.GrantedRole)
		}
		listing.AssignRoles(rolesByLogin)
	case ctx.Err() != nil:
		return nil, ctx.Err()
	case dwhexecoracle.IsPermissionError(err):
		listing.Skip(scrapper.PlatformUserFactRoles, scrapper.PlatformUserSkipRefused, "DBA_ROLE_PRIVS was refused ("+platformUsersGrant+")")
	case isUnavailable(err):
		listing.Skip(
			scrapper.PlatformUserFactRoles,
			scrapper.PlatformUserSkipUnavailable,
			"this Oracle version cannot read DBA_ROLE_PRIVS: "+err.Error(),
		)
	default:
		logging.GetLogger(ctx).WithError(err).Warnf("cannot read %s, listing users without roles", views.roleGrant)
		listing.Skip(scrapper.PlatformUserFactRoles, scrapper.PlatformUserSkipFailed, "reading DBA_ROLE_PRIVS failed: "+err.Error())
	}

	return scrapper.CollectPlatformUsers(ctx, notAnswered, listing)
}

func selectOracleUsers(ctx context.Context, q rowQuerier, query, source string) ([]*oracleUserRow, error) {
	rows, err := q.QueryRows(ctx, query)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to list users from %s", source)
	}
	defer rows.Close()
	return scrapper.ScanAll[oracleUserRow](ctx, rows, source)
}

func selectRoleGrants(ctx context.Context, q rowQuerier, view string) ([]*oracleRoleGrantRow, error) {
	rows, err := q.QueryRows(ctx, fmt.Sprintf(roleGrantsSql, view))
	if err != nil {
		return nil, errors.Wrapf(err, "failed to list role grants from %s", view)
	}
	defer rows.Close()
	return scrapper.ScanAll[oracleRoleGrantRow](ctx, rows, view)
}

func (r *oracleUserRow) toPlatformUser() *scrapper.PlatformUser {
	u := &scrapper.PlatformUser{Login: r.Username, Type: r.AuthenticationType.String}
	if r.UserId.Valid {
		u.PlatformId = strconv.FormatInt(r.UserId.Int64, 10)
	}
	if r.AccountStatus.Valid {
		disabled := oracleAccountDisabled(r.AccountStatus.String, r.AuthenticationType.String)
		u.Disabled = &disabled
	}
	u.CreatedAt = utcWallClock(r.CreatedUtc)
	u.LastLoginAt = utcWallClock(r.LastLoginUtc)
	return u
}

// oracleAccountDisabled reads ACCOUNT_STATUS: a locked account (LOCKED,
// LOCKED(TIMED), EXPIRED & LOCKED, ...) cannot sign in, and neither can one
// whose password expired, unless it is still in its grace period
// (EXPIRED(GRACE)). A schema-only account (AUTHENTICATION_TYPE NONE) has no
// way to authenticate at all.
func oracleAccountDisabled(status, authenticationType string) bool {
	status = strings.ToUpper(status)
	if strings.EqualFold(authenticationType, "NONE") {
		return true
	}
	if strings.Contains(status, "LOCKED") {
		return true
	}
	return strings.Contains(status, "EXPIRED") && !strings.Contains(status, "GRACE")
}

// utcWallClock reads a zone-less timestamp SYS_EXTRACT_UTC produced as UTC,
// whatever zone the driver labelled it with.
func utcWallClock(t sql.NullTime) *time.Time {
	if !t.Valid {
		return nil
	}
	v := time.Date(t.Time.Year(), t.Time.Month(), t.Time.Day(), t.Time.Hour(), t.Time.Minute(), t.Time.Second(), t.Time.Nanosecond(), time.UTC)
	return &v
}
