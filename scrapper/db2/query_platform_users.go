package db2

import (
	"context"
	"strings"

	"github.com/getsynq/dwhsupport/logging"
	"github.com/getsynq/dwhsupport/scrapper"
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
// about a login), so it is read only when the admin view is refused.
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

const sessionUserSql = `VALUES SESSION_USER`

const roleAuthSql = `SELECT TRIM(grantee) AS grantee, TRIM(rolename) AS rolename FROM SYSCAT.ROLEAUTH WHERE granteetype = 'U'`

type db2AuthIdRow struct {
	AuthId string `db:"AUTHID"`
}

type db2RoleAuthRow struct {
	Grantee  string `db:"GRANTEE"`
	RoleName string `db:"ROLENAME"`
}

// QueryPlatformUsers lists the authorization IDs of type user the database
// knows from its grants, with the roles granted to each. Db2 has no catalog
// of users (see platformUsersCompletenessReason), so the listing is never
// known to be complete and only roles can be read about a user. Groups
// (GRANTEETYPE G) and roles are not users and are not listed; a role granted
// to a group does not reach the users of that group either, as their
// membership lives outside the database.
//
// The first of authIdSources the role may read is the listing; each one
// refused before it is reported as a refused source.
func (e *Db2Scrapper) QueryPlatformUsers(ctx context.Context) (*scrapper.PlatformUsers, error) {
	return e.queryPlatformUsers(ctx, authIdSources)
}

func (e *Db2Scrapper) queryPlatformUsers(ctx context.Context, sources []authIdSource) (*scrapper.PlatformUsers, error) {
	var listings []*scrapper.PlatformUserListing
	var lastErr error
	for _, source := range sources {
		authIds, err := e.selectAuthIds(ctx, source)
		if err != nil {
			if !e.IsPermissionError(err) {
				return nil, err
			}
			logging.GetLogger(ctx).WithError(err).Warnf("cannot read %s", source.object)
			listings = append(listings, scrapper.RefusedPlatformUserSource(source.source, scrapper.PlatformUserSourceSQL, err))
			lastErr = err
			continue
		}
		listings = append(listings, e.authIdListing(ctx, source, authIds))
		break
	}
	users := scrapper.NewPlatformUsers(listings...)
	if users.AllRefused() && lastErr != nil {
		return nil, lastErr
	}
	return users, nil
}

func (e *Db2Scrapper) authIdListing(ctx context.Context, source authIdSource, authIds []string) *scrapper.PlatformUserListing {
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
	var sessionUser []string
	if err := e.executor.Select(ctx, &sessionUser, sessionUserSql); err != nil {
		logging.GetLogger(ctx).WithError(err).Warn("cannot read SESSION_USER, listing users without it")
	} else {
		for _, u := range sessionUser {
			listing.Users = append(listing.Users, &scrapper.PlatformUser{Login: strings.TrimSpace(u)})
		}
	}

	rolesByLogin, err := e.selectRoleGrants(ctx)
	switch {
	case err == nil:
		listing.AssignRoles(rolesByLogin)
	case e.IsPermissionError(err):
		listing.Skip(scrapper.PlatformUserFactRoles, "SYSCAT.ROLEAUTH was refused ("+platformUsersGrant+")")
	default:
		logging.GetLogger(ctx).WithError(err).Warn("cannot read SYSCAT.ROLEAUTH, listing users without roles")
		listing.Skip(scrapper.PlatformUserFactRoles, "reading SYSCAT.ROLEAUTH failed: "+err.Error())
	}
	return listing
}

func (e *Db2Scrapper) selectAuthIds(ctx context.Context, source authIdSource) ([]string, error) {
	rows, err := e.executor.QueryRows(ctx, source.query)
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
		authIds = append(authIds, r.AuthId)
	}
	return authIds, nil
}

func (e *Db2Scrapper) selectRoleGrants(ctx context.Context) (map[string][]string, error) {
	rows, err := e.executor.QueryRows(ctx, roleAuthSql)
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
