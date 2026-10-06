package postgres

import (
	"context"

	"github.com/getsynq/dwhsupport/exec/querystats"
	"github.com/getsynq/dwhsupport/logging"
	"github.com/getsynq/dwhsupport/scrapper"
)

// platformUsersGrant names no grant: pg_roles and pg_auth_members are readable
// by every role, so any login sees every user and every fact.
const platformUsersGrant = "no grant needed: every role can read pg_roles and pg_auth_members"

// platformUsersSQL lists the roles that can log in. A NOLOGIN role is a group
// in Postgres terms: it is reported as a role its members hold, never as a
// user, because nothing can authenticate as it. The consequence is that a
// user switched to NOLOGIN drops out of the listing rather than being reported
// disabled; Postgres keeps no flag that tells such a user from a group.
//
// Disabled is the password expiry (VALID UNTIL) having passed. Postgres has no
// other disabled state, and VALID UNTIL only stops password authentication, so
// a login using another method (certificate, IAM, trust) still connects.
//
// The default role is a `role` setting made with ALTER ROLE ... SET role = x
// (rolconfig), the only way Postgres starts a session in a role other than the
// login itself. A setting made per database (ALTER ROLE ... IN DATABASE) is
// not read.
const platformUsersSQL = `
SELECT
	r.rolname AS login,
	r.oid::text AS platform_id,
	COALESCE(shobj_description(r.oid, 'pg_authid'), '') AS comment,
	(r.rolvaliduntil IS NOT NULL AND r.rolvaliduntil < now()) AS disabled,
	COALESCE((SELECT substr(c, 6) FROM unnest(r.rolconfig) AS c WHERE c LIKE 'role=%' LIMIT 1), '') AS default_role
FROM pg_roles r
WHERE r.rolcanlogin`

// platformUserRolesSQL lists the roles each login is a direct member of.
const platformUserRolesSQL = `
SELECT member.rolname AS login, granted.rolname AS role
FROM pg_auth_members m
JOIN pg_roles member ON member.oid = m.member
JOIN pg_roles granted ON granted.oid = m.roleid
WHERE member.rolcanlogin`

type platformUserRow struct {
	Login       string `db:"login"`
	PlatformId  string `db:"platform_id"`
	Comment     string `db:"comment"`
	Disabled    bool   `db:"disabled"`
	DefaultRole string `db:"default_role"`
}

type platformUserRoleRow struct {
	Login string `db:"login"`
	Role  string `db:"role"`
}

// skippedPlatformUserFacts are the facts Postgres does not keep about a role.
var skippedPlatformUserFacts = []scrapper.SkippedPlatformUserFact{
	{Fact: scrapper.PlatformUserFactType, Reason: "Postgres has no user type"},
	{Fact: scrapper.PlatformUserFactEmail, Reason: "Postgres keeps no email for a role"},
	{Fact: scrapper.PlatformUserFactDisplayName, Reason: "Postgres keeps no display name for a role"},
	{Fact: scrapper.PlatformUserFactCreatedAt, Reason: "Postgres does not record when a role was created"},
	{Fact: scrapper.PlatformUserFactLastLoginAt, Reason: "Postgres does not record logins"},
}

// QueryPlatformUsers lists the roles that can log in. Every role can read
// pg_roles, so the listing is complete and cannot be refused.
func (e *PostgresScrapper) QueryPlatformUsers(ctx context.Context) (*scrapper.PlatformUsers, error) {
	collector, ctx := querystats.Start(ctx)
	defer collector.Finish()

	rows, err := e.executor.QueryRows(ctx, platformUsersSQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	listed, err := scrapper.ScanAll[platformUserRow](ctx, rows, "pg_roles")
	if err != nil {
		return nil, err
	}

	users := &scrapper.PlatformUserListing{
		Source:       "postgres.pg_roles",
		Kind:         scrapper.PlatformUserSourceSQL,
		Completeness: scrapper.PlatformUsersComplete,
	}
	for _, row := range listed {
		users.Users = append(users.Users, row.toPlatformUser())
	}
	for _, skipped := range skippedPlatformUserFacts {
		users.Skip(skipped.Fact, skipped.Reason)
	}

	if roles, err := e.queryPlatformUserRoles(ctx); err != nil {
		logging.GetLogger(ctx).WithError(err).Warn("failed to read postgres role memberships, listing users without roles")
		users.Skip(scrapper.PlatformUserFactRoles, "reading pg_auth_members failed: "+err.Error())
	} else {
		users.AssignRoles(roles)
	}

	collector.SetRowsProduced(int64(len(users.Users)))
	return scrapper.NewPlatformUsers(users), nil
}

func (e *PostgresScrapper) queryPlatformUserRoles(ctx context.Context) (map[string][]string, error) {
	rows, err := e.executor.QueryRows(ctx, platformUserRolesSQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	memberships, err := scrapper.ScanAll[platformUserRoleRow](ctx, rows, "pg_auth_members")
	if err != nil {
		return nil, err
	}
	return rolesByLogin(memberships), nil
}

func rolesByLogin(memberships []*platformUserRoleRow) map[string][]string {
	roles := map[string][]string{}
	for _, m := range memberships {
		roles[m.Login] = append(roles[m.Login], m.Role)
	}
	return roles
}

func (r *platformUserRow) toPlatformUser() *scrapper.PlatformUser {
	disabled := r.Disabled
	return &scrapper.PlatformUser{
		Login:       r.Login,
		PlatformId:  r.PlatformId,
		Comment:     r.Comment,
		Disabled:    &disabled,
		DefaultRole: r.DefaultRole,
	}
}
