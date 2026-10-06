package oracle

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/getsynq/dwhsupport/logging"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/pkg/errors"
)

// platformUsersGrant is what lets a role read DBA_USERS and DBA_ROLE_PRIVS.
const platformUsersGrant = "SELECT_CATALOG_ROLE (or SELECT ANY DICTIONARY), for DBA_USERS and DBA_ROLE_PRIVS"

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

// Source names of the Oracle user listings.
const (
	oracleDbaUsersSource = "oracle.dba_users"
	oracleAllUsersSource = "oracle.all_users"
)

// QueryPlatformUsers lists the database users from DBA_USERS. Login is
// USERNAME, the name V$SQL reports as PARSING_SCHEMA_NAME in query logs; Type
// is AUTHENTICATION_TYPE (PASSWORD, EXTERNAL, GLOBAL, NONE). Oracle-maintained
// users (SYS, SYSTEM, ...) are listed too: they are logins, and SYSTEM in
// particular runs queries. Roles come from DBA_ROLE_PRIVS.
//
// ALL_USERS lists the same users with only their name, id and creation time,
// so it is read only when DBA_USERS is refused, and the refusal is reported as
// a source of its own.
func (e *OracleScrapper) QueryPlatformUsers(ctx context.Context) (*scrapper.PlatformUsers, error) {
	return e.queryPlatformUsers(ctx, defaultPlatformUserViews)
}

func (e *OracleScrapper) queryPlatformUsers(ctx context.Context, views platformUserViews) (*scrapper.PlatformUsers, error) {
	listing := &scrapper.PlatformUserListing{
		Source:       oracleDbaUsersSource,
		Kind:         scrapper.PlatformUserSourceSQL,
		Completeness: scrapper.PlatformUsersComplete,
	}
	var refused *scrapper.PlatformUserListing

	rows, err := e.selectOracleUsers(ctx, fmt.Sprintf(dbaUsersSql, views.users), views.users)
	if err != nil {
		if !e.IsPermissionError(err) {
			return nil, err
		}
		logging.GetLogger(ctx).WithError(err).Warnf("cannot read %s, listing users from ALL_USERS", views.users)
		refused = scrapper.RefusedPlatformUserSource(oracleDbaUsersSource, scrapper.PlatformUserSourceSQL, err)
		listing.Source = oracleAllUsersSource
		rows, err = e.selectOracleUsers(ctx, allUsersSql, "ALL_USERS")
		if err != nil {
			// Every session may read ALL_USERS; failing here leaves no source.
			return nil, err
		}
		reason := "ALL_USERS has no such column; DBA_USERS has it (" + platformUsersGrant + ")"
		listing.Skip(scrapper.PlatformUserFactType, reason)
		listing.Skip(scrapper.PlatformUserFactDisabled, reason)
		listing.Skip(scrapper.PlatformUserFactLastLoginAt, reason)
	}
	listing.Skip(scrapper.PlatformUserFactEmail, "Oracle keeps no email for a database user")
	listing.Skip(scrapper.PlatformUserFactDisplayName, "Oracle keeps no display name for a database user")
	listing.Skip(scrapper.PlatformUserFactComment, "Oracle keeps no comment on a database user")
	listing.Skip(
		scrapper.PlatformUserFactDefaultRole,
		"Oracle enables every default role of a user at once, so there is no single default role; see roles",
	)

	for _, row := range rows {
		listing.Users = append(listing.Users, row.toPlatformUser())
	}

	grants, err := e.selectRoleGrants(ctx, views.roleGrant)
	switch {
	case err == nil:
		rolesByLogin := map[string][]string{}
		for _, g := range grants {
			rolesByLogin[g.Grantee] = append(rolesByLogin[g.Grantee], g.GrantedRole)
		}
		listing.AssignRoles(rolesByLogin)
	case e.IsPermissionError(err):
		listing.Skip(scrapper.PlatformUserFactRoles, "DBA_ROLE_PRIVS was refused ("+platformUsersGrant+")")
	default:
		logging.GetLogger(ctx).WithError(err).Warnf("cannot read %s, listing users without roles", views.roleGrant)
		listing.Skip(scrapper.PlatformUserFactRoles, "reading DBA_ROLE_PRIVS failed: "+err.Error())
	}

	return scrapper.NewPlatformUsers(refused, listing), nil
}

func (e *OracleScrapper) selectOracleUsers(ctx context.Context, query, source string) ([]*oracleUserRow, error) {
	rows, err := e.executor.QueryRows(ctx, query)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to list users from %s", source)
	}
	defer rows.Close()
	return scrapper.ScanAll[oracleUserRow](ctx, rows, source)
}

func (e *OracleScrapper) selectRoleGrants(ctx context.Context, view string) ([]*oracleRoleGrantRow, error) {
	rows, err := e.executor.QueryRows(ctx, fmt.Sprintf(roleGrantsSql, view))
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
