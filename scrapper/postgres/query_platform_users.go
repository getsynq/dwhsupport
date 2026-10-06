package postgres

import (
	"context"

	dwhexecpostgres "github.com/getsynq/dwhsupport/exec/postgres"
	"github.com/getsynq/dwhsupport/exec/querystats"
	"github.com/getsynq/dwhsupport/logging"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/lib/pq"
	"github.com/pkg/errors"
)

// platformUsersGrant names no grant: pg_roles and pg_auth_members are readable
// by every role, so any login sees every user and every fact.
const platformUsersGrant = "no grant needed: every role can read pg_roles and pg_auth_members"

const platformUsersSource = "postgres.pg_roles"

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
// It reads only the columns every Postgres and Postgres-compatible engine has;
// the comment and the default role are read apart, so an engine without
// shobj_description or rolconfig loses that fact and not the listing.
const platformUsersSQL = `
SELECT
	r.rolname AS login,
	r.oid::text AS platform_id,
	(r.rolvaliduntil IS NOT NULL AND r.rolvaliduntil < now()) AS disabled
FROM pg_roles r
WHERE r.rolcanlogin`

// platformUserCommentsSQL reads the COMMENT ON ROLE of each login.
const platformUserCommentsSQL = `
SELECT r.rolname AS login, shobj_description(r.oid, 'pg_authid') AS value
FROM pg_roles r
WHERE r.rolcanlogin AND shobj_description(r.oid, 'pg_authid') IS NOT NULL`

// platformUserDefaultRolesSQL reads the default role: a `role` setting made
// with ALTER ROLE ... SET role = x (rolconfig), the only way Postgres starts a
// session in a role other than the login itself. A setting made per database
// (ALTER ROLE ... IN DATABASE) is not read.
const platformUserDefaultRolesSQL = `
SELECT r.rolname AS login, substr(c, 6) AS value
FROM pg_roles r, unnest(r.rolconfig) AS c
WHERE r.rolcanlogin AND c LIKE 'role=%'`

// platformUserRolesSQL lists the roles each login is a direct member of.
const platformUserRolesSQL = `
SELECT member.rolname AS login, granted.rolname AS value
FROM pg_auth_members m
JOIN pg_roles member ON member.oid = m.member
JOIN pg_roles granted ON granted.oid = m.roleid
WHERE member.rolcanlogin`

type platformUserRow struct {
	Login      string `db:"login"`
	PlatformId string `db:"platform_id"`
	Disabled   bool   `db:"disabled"`
}

// platformUserValueRow is one fact of one login, read by a fact query.
type platformUserValueRow struct {
	Login string `db:"login"`
	Value string `db:"value"`
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
// pg_roles, so the listing is complete. Nothing a grant or the server version
// decides fails the call: a listing that could not be read is returned in its
// refused, unavailable or failed state, and a fact that could not be read is
// skipped with the reason.
func (e *PostgresScrapper) QueryPlatformUsers(ctx context.Context) (*scrapper.PlatformUsers, error) {
	collector, ctx := querystats.Start(ctx)
	defer collector.Finish()

	listed, err := e.scanPlatformUserQuery(ctx, platformUsersSQL, "pg_roles")
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

	facts := []struct {
		fact   scrapper.PlatformUserFact
		sql    string
		source string
		apply  func(u *scrapper.PlatformUser, value string)
	}{
		{scrapper.PlatformUserFactComment, platformUserCommentsSQL, "shobj_description", func(u *scrapper.PlatformUser, v string) { u.Comment = v }},
		{
			scrapper.PlatformUserFactDefaultRole,
			platformUserDefaultRolesSQL,
			"pg_roles.rolconfig",
			func(u *scrapper.PlatformUser, v string) { u.DefaultRole = v },
		},
		{
			scrapper.PlatformUserFactRoles,
			platformUserRolesSQL,
			"pg_auth_members",
			func(u *scrapper.PlatformUser, v string) { u.Roles = append(u.Roles, v) },
		},
	}
	for _, f := range facts {
		values, err := e.queryPlatformUserValues(ctx, f.sql, f.source)
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return nil, ctxErr
			}
			logging.GetLogger(ctx).WithError(err).Warnf("failed to read postgres %s of users, listing them without it", f.fact)
			users.Skip(f.fact, platformUserFactSkipReason(f.source, err))
			continue
		}
		for _, u := range users.Users {
			for _, v := range values[u.Login] {
				f.apply(u, v)
			}
		}
	}

	collector.SetRowsProduced(int64(len(users.Users)))
	return scrapper.CollectPlatformUsers(ctx, users)
}

func (e *PostgresScrapper) scanPlatformUserQuery(ctx context.Context, sql, source string) ([]*platformUserRow, error) {
	rows, err := e.executor.QueryRows(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scrapper.ScanAll[platformUserRow](ctx, rows, source)
}

// queryPlatformUserValues runs a fact query and groups its values by login.
func (e *PostgresScrapper) queryPlatformUserValues(ctx context.Context, sql, source string) (map[string][]string, error) {
	rows, err := e.executor.QueryRows(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	scanned, err := scrapper.ScanAll[platformUserValueRow](ctx, rows, source)
	if err != nil {
		return nil, err
	}
	values := map[string][]string{}
	for _, r := range scanned {
		values[r.Login] = append(values[r.Login], r.Value)
	}
	return values, nil
}

func platformUsersSourceError(err error) *scrapper.PlatformUserListing {
	return scrapper.PlatformUserSourceError(
		platformUsersSource, scrapper.PlatformUserSourceSQL, err, dwhexecpostgres.IsPermissionError, isUnavailableError,
	)
}

// isUnavailableError reports whether err says this server lacks the view,
// column or function a query names: an older Postgres, or a Postgres-compatible
// engine that does not carry the whole catalog. No grant fixes it.
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
// three states a listing has: refused, unavailable on this server, failed.
func platformUserFactSkipReason(source string, err error) string {
	switch {
	case dwhexecpostgres.IsPermissionError(err):
		return "reading " + source + " was refused: " + err.Error()
	case isUnavailableError(err):
		return source + " is not available on this server: " + err.Error()
	default:
		return "reading " + source + " failed: " + err.Error()
	}
}
