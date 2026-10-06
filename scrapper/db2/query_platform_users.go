package db2

import (
	"context"
	"regexp"
	"strings"

	dwhexecdb2 "github.com/getsynq/dwhsupport/exec/db2"
	"github.com/getsynq/dwhsupport/logging"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

// platformUsersGrant is what lets a role read the authorization IDs and their
// roles. PUBLIC holds it unless the database was created RESTRICTIVE.
const platformUsersGrant = "SELECT on SYSIBMADM.AUTHORIZATIONIDS and SYSCAT.ROLEAUTH (PUBLIC has both unless the database was created RESTRICTIVE)"

// platformUsersCompletenessReason is why a Db2 listing is never known to be
// complete: no grant changes it.
const platformUsersCompletenessReason = "Db2 authenticates users outside the database (operating system, LDAP or a security plugin) " +
	"and keeps no list of them; the listing holds the authorization IDs the database granted something to directly, " +
	"plus the one we connect as, so a user who connects through a grant to PUBLIC or to a group is not listed"

const db2NoUserRecord = "Db2 authenticates users outside the database and keeps no record of them beyond their grants"

type authIdSource struct {
	// source is the listing's name, object what is read, for messages.
	source string
	object string
	query  string
}

// SYSIBMADM.AUTHORIZATIONIDS gathers every authorization ID any SYSCAT.*AUTH
// view names. The SYSCAT union lists a subset of it (the grants that say most
// about a login), so it is read only when the admin view did not answer.
var authIdSources = []authIdSource{
	{
		source: "db2.sysibmadm.authorizationids",
		object: "SYSIBMADM.AUTHORIZATIONIDS",
		query:  `SELECT TRIM(authid) AS authid FROM SYSIBMADM.AUTHORIZATIONIDS WHERE authidtype = 'U'`,
	},
	{
		source: "db2.syscat_auth",
		object: "SYSCAT.DBAUTH, ROLEAUTH, SCHEMAAUTH and TABAUTH",
		query: `SELECT TRIM(grantee) AS authid FROM SYSCAT.DBAUTH WHERE granteetype = 'U'
UNION SELECT TRIM(grantee) FROM SYSCAT.ROLEAUTH WHERE granteetype = 'U'
UNION SELECT TRIM(grantee) FROM SYSCAT.SCHEMAAUTH WHERE granteetype = 'U'
UNION SELECT TRIM(grantee) FROM SYSCAT.TABAUTH WHERE granteetype = 'U'`,
	},
}

const sessionUserSql = `SELECT TRIM(SESSION_USER) AS authid FROM SYSIBM.SYSDUMMY1`

const roleAuthSql = `SELECT TRIM(grantee) AS grantee, TRIM(rolename) AS rolename FROM SYSCAT.ROLEAUTH WHERE granteetype = 'U'`

type db2AuthIdRow struct {
	AuthId string `db:"AUTHID"`
}

type db2RoleAuthRow struct {
	Grantee  string `db:"GRANTEE"`
	RoleName string `db:"ROLENAME"`
}

// rowQuerier is the part of the executor the listing needs, so the tests can
// answer it with sqlmock.
type rowQuerier interface {
	QueryRows(ctx context.Context, sql string, args ...interface{}) (*sqlx.Rows, error)
}

// unavailableErrors matches what Db2 answers a name this version or install
// does not have with: -204 / 42704 an undefined object (SYSIBMADM views are
// absent from some installs), -206 / 42703 an undefined column (SYSCAT.DBAUTH
// grew SECADMAUTH and others over versions), -440 / 42884 an undefined
// routine. The separators follow exec/db2's permissionErrors.
var unavailableErrors = regexp.MustCompile(`SQLCODE[=:]\s*-(204|206|440)\b|SQLSTATE[=:]\s*(42704|42703|42884)\b`)

// isUnavailable reports an error that says this Db2 has no such view, column or
// routine.
func isUnavailable(err error) bool {
	if err == nil {
		return false
	}
	return unavailableErrors.MatchString(err.Error())
}

// QueryPlatformUsers lists the authorization IDs of type user the database
// knows from its grants, with the roles granted to each. Db2 has no catalog
// of users (see platformUsersCompletenessReason), so the listing is never
// known to be complete and only roles can be read about a user. Groups
// (GRANTEETYPE G) and roles are not users and are not listed; a role granted
// to a group does not reach the users of that group either, as their
// membership lives outside the database.
//
// The first of authIdSources that answers is the listing. Each one before it
// that did not is reported as refused, unavailable or failed, and the next is
// read whatever the reason: a failure of one view (a timeout, a decode error)
// says nothing about the others, and a connection that is down fails them all,
// which fails the call.
func (e *Db2Scrapper) QueryPlatformUsers(ctx context.Context) (*scrapper.PlatformUsers, error) {
	return listDb2PlatformUsers(ctx, e.executor, authIdSources)
}

func listDb2PlatformUsers(ctx context.Context, q rowQuerier, sources []authIdSource) (*scrapper.PlatformUsers, error) {
	var listings []*scrapper.PlatformUserListing
	for _, source := range sources {
		authIds, err := selectAuthIds(ctx, q, source)
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		if err != nil {
			logging.GetLogger(ctx).WithError(err).Warnf("cannot read %s", source.object)
			listings = append(listings, scrapper.PlatformUserSourceError(
				source.source, scrapper.PlatformUserSourceSQL, err, dwhexecdb2.IsPermissionError, isUnavailable,
			))
			continue
		}
		listing, err := authIdListing(ctx, q, source, authIds)
		if err != nil {
			return nil, err
		}
		listings = append(listings, listing)
		break
	}
	return scrapper.CollectPlatformUsers(ctx, listings...)
}

// authIdListing adds what is known about each authorization ID. Nothing here
// fails the listing; only a context that is done does.
func authIdListing(ctx context.Context, q rowQuerier, source authIdSource, authIds []string) (*scrapper.PlatformUserListing, error) {
	listing := &scrapper.PlatformUserListing{
		Source:             source.source,
		Kind:               scrapper.PlatformUserSourceSQL,
		Completeness:       scrapper.PlatformUsersUnknown,
		CompletenessReason: platformUsersCompletenessReason,
	}
	for _, fact := range []scrapper.PlatformUserFact{
		scrapper.PlatformUserFactPlatformId,
		scrapper.PlatformUserFactType,
		scrapper.PlatformUserFactEmail,
		scrapper.PlatformUserFactDisplayName,
		scrapper.PlatformUserFactComment,
		scrapper.PlatformUserFactDisabled,
		scrapper.PlatformUserFactCreatedAt,
		scrapper.PlatformUserFactLastLoginAt,
		scrapper.PlatformUserFactDefaultRole,
	} {
		listing.Skip(fact, db2NoUserRecord)
	}
	for _, authId := range authIds {
		listing.Users = append(listing.Users, &scrapper.PlatformUser{Login: authId})
	}

	// The user we connect as authenticated, so it is a login even when it
	// holds nothing but what PUBLIC holds.
	sessionUser, err := selectAuthIds(ctx, q, authIdSource{object: "SESSION_USER", query: sessionUserSql})
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if err != nil {
		logging.GetLogger(ctx).WithError(err).Warn("cannot read SESSION_USER, listing users without it")
	}
	for _, u := range sessionUser {
		listing.Users = append(listing.Users, &scrapper.PlatformUser{Login: u})
	}

	rolesByLogin, err := selectRoleGrants(ctx, q)
	switch {
	case err == nil:
		listing.AssignRoles(rolesByLogin)
	case ctx.Err() != nil:
		return nil, ctx.Err()
	case dwhexecdb2.IsPermissionError(err):
		listing.Skip(scrapper.PlatformUserFactRoles, "SYSCAT.ROLEAUTH was refused ("+platformUsersGrant+")")
	case isUnavailable(err):
		listing.Skip(scrapper.PlatformUserFactRoles, "this Db2 cannot read SYSCAT.ROLEAUTH: "+err.Error())
	default:
		logging.GetLogger(ctx).WithError(err).Warn("cannot read SYSCAT.ROLEAUTH, listing users without roles")
		listing.Skip(scrapper.PlatformUserFactRoles, "reading SYSCAT.ROLEAUTH failed: "+err.Error())
	}
	return listing, nil
}

func selectAuthIds(ctx context.Context, q rowQuerier, source authIdSource) ([]string, error) {
	rows, err := q.QueryRows(ctx, source.query)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to list authorization IDs from %s", source.object)
	}
	defer rows.Close()
	scanned, err := scrapper.ScanAll[db2AuthIdRow](ctx, rows, source.object)
	if err != nil {
		return nil, err
	}
	authIds := make([]string, 0, len(scanned))
	for _, r := range scanned {
		authIds = append(authIds, strings.TrimSpace(r.AuthId))
	}
	return authIds, nil
}

func selectRoleGrants(ctx context.Context, q rowQuerier) (map[string][]string, error) {
	rows, err := q.QueryRows(ctx, roleAuthSql)
	if err != nil {
		return nil, errors.Wrap(err, "failed to list role grants from SYSCAT.ROLEAUTH")
	}
	defer rows.Close()
	grants, err := scrapper.ScanAll[db2RoleAuthRow](ctx, rows, "SYSCAT.ROLEAUTH")
	if err != nil {
		return nil, err
	}
	rolesByLogin := map[string][]string{}
	for _, g := range grants {
		rolesByLogin[g.Grantee] = append(rolesByLogin[g.Grantee], g.RoleName)
	}
	return rolesByLogin, nil
}
