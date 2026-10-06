package snowflake

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/getsynq/dwhsupport/logging"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/pkg/errors"
)

// platformUsersGrant is what lets a role read ACCOUNT_USAGE.USERS and
// ACCOUNT_USAGE.GRANTS_TO_USERS, which hold every user and every fact. Without
// it the listing falls back to SHOW USERS.
const platformUsersGrant = "IMPORTED PRIVILEGES on database SNOWFLAKE (or the SNOWFLAKE.USAGE_VIEWER database role), " +
	"to read SNOWFLAKE.ACCOUNT_USAGE.USERS and GRANTS_TO_USERS"

// showUsersPageSize is the most rows one SHOW USERS returns; a full page is
// followed by another starting after its last name.
const showUsersPageSize = 10000

// accountUsageUsersQuery lists the account's users that are not dropped.
// ACCOUNT_USAGE lags behind by up to two hours, so a user created since is not
// in it yet.
const accountUsageUsersQuery = `SELECT
	USER_ID,
	NAME,
	TYPE,
	EMAIL,
	DISPLAY_NAME,
	FIRST_NAME,
	LAST_NAME,
	COMMENT,
	DISABLED,
	SNOWFLAKE_LOCK,
	EXPIRES_AT,
	CREATED_ON,
	LAST_SUCCESS_LOGIN,
	DEFAULT_ROLE
FROM %s.ACCOUNT_USAGE.USERS
WHERE DELETED_ON IS NULL`

// accountUsageUserRolesQuery lists the roles granted to users directly.
const accountUsageUserRolesQuery = `SELECT GRANTEE_NAME, ROLE
FROM %s.ACCOUNT_USAGE.GRANTS_TO_USERS
WHERE DELETED_ON IS NULL`

type accountUsageUserRow struct {
	UserId           sql.NullString `db:"USER_ID"`
	Name             string         `db:"NAME"`
	Type             sql.NullString `db:"TYPE"`
	Email            sql.NullString `db:"EMAIL"`
	DisplayName      sql.NullString `db:"DISPLAY_NAME"`
	FirstName        sql.NullString `db:"FIRST_NAME"`
	LastName         sql.NullString `db:"LAST_NAME"`
	Comment          sql.NullString `db:"COMMENT"`
	Disabled         sql.NullBool   `db:"DISABLED"`
	SnowflakeLock    sql.NullBool   `db:"SNOWFLAKE_LOCK"`
	ExpiresAt        sql.NullTime   `db:"EXPIRES_AT"`
	CreatedOn        sql.NullTime   `db:"CREATED_ON"`
	LastSuccessLogin sql.NullTime   `db:"LAST_SUCCESS_LOGIN"`
	DefaultRole      sql.NullString `db:"DEFAULT_ROLE"`
}

type userRoleRow struct {
	GranteeName string `db:"GRANTEE_NAME"`
	Role        string `db:"ROLE"`
}

// showUsersRow is what SHOW USERS returns. For a user the role neither owns
// nor holds MANAGE GRANTS on, Snowflake fills only name, created_on, owner and
// last_success_login and leaves every other column NULL. The booleans come
// back as the strings "true" / "false".
type showUsersRow struct {
	Name             string         `db:"name"`
	Type             sql.NullString `db:"type"`
	Email            sql.NullString `db:"email"`
	DisplayName      sql.NullString `db:"display_name"`
	FirstName        sql.NullString `db:"first_name"`
	LastName         sql.NullString `db:"last_name"`
	Comment          sql.NullString `db:"comment"`
	Disabled         sql.NullString `db:"disabled"`
	SnowflakeLock    sql.NullString `db:"snowflake_lock"`
	ExpiresAt        sql.NullTime   `db:"expires_at_time"`
	CreatedOn        sql.NullTime   `db:"created_on"`
	LastSuccessLogin sql.NullTime   `db:"last_success_login"`
	DefaultRole      sql.NullString `db:"default_role"`
}

// Sources of Snowflake users, in trust order.
const (
	accountUsageUsersSource = "snowflake.account_usage.users"
	showUsersSource         = "snowflake.show_users"
)

// QueryPlatformUsers reads two sources and keeps them apart.
// ACCOUNT_USAGE.USERS, with the roles from ACCOUNT_USAGE.GRANTS_TO_USERS, has
// every fact but lags behind by up to two hours. SHOW USERS is current, so it
// names a user created since, but hides the details of the users the role
// does not own. Either may be refused; both refused is the permission error.
//
// Login is the user's NAME, which is what QUERY_HISTORY.USER_NAME reports, not
// its LOGIN_NAME.
func (e *SnowflakeScrapper) QueryPlatformUsers(ctx context.Context) (*scrapper.PlatformUsers, error) {
	accountUsage, accountUsageErr := e.queryAccountUsageUsers(ctx)
	if accountUsageErr != nil {
		if !e.IsPermissionError(accountUsageErr) {
			return nil, accountUsageErr
		}
		logging.GetLogger(ctx).WithError(accountUsageErr).Info("cannot read ACCOUNT_USAGE.USERS, listing users with SHOW USERS only")
		accountUsage = scrapper.RefusedPlatformUserSource(accountUsageUsersSource, scrapper.PlatformUserSourceSQL, accountUsageErr)
	}

	show, showErr := e.queryShowUsers(ctx)
	if showErr != nil {
		if !e.IsPermissionError(showErr) {
			return nil, showErr
		}
		show = scrapper.RefusedPlatformUserSource(showUsersSource, scrapper.PlatformUserSourceSQL, showErr)
	}

	result := scrapper.NewPlatformUsers(accountUsage, show)
	if result.AllRefused() {
		return nil, errors.Wrapf(showErr, "SHOW USERS refused, as was ACCOUNT_USAGE.USERS (%v)", accountUsageErr)
	}
	return result, nil
}

func (e *SnowflakeScrapper) queryAccountUsageUsers(ctx context.Context) (*scrapper.PlatformUserListing, error) {
	rows, err := e.executor.QueryRows(ctx, fmt.Sprintf(accountUsageUsersQuery, e.accountUsageDb()))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	userRows, err := scrapper.ScanAll[accountUsageUserRow](ctx, rows, "ACCOUNT_USAGE.USERS")
	if err != nil {
		return nil, err
	}

	now := time.Now()
	users := &scrapper.PlatformUserListing{
		Source:       accountUsageUsersSource,
		Kind:         scrapper.PlatformUserSourceSQL,
		Completeness: scrapper.PlatformUsersComplete,
	}
	for _, r := range userRows {
		users.Users = append(users.Users, &scrapper.PlatformUser{
			Login:       r.Name,
			PlatformId:  r.UserId.String,
			Type:        r.Type.String,
			Email:       r.Email.String,
			DisplayName: displayName(r.DisplayName, r.FirstName, r.LastName, r.Name),
			Comment:     r.Comment.String,
			Disabled:    isDisabled(r.Disabled, r.SnowflakeLock, r.ExpiresAt, now),
			CreatedAt:   utcTime(r.CreatedOn),
			LastLoginAt: utcTime(r.LastSuccessLogin),
			DefaultRole: r.DefaultRole.String,
		})
	}

	roles, err := e.queryAccountUsageUserRoles(ctx)
	if err != nil {
		if !e.IsPermissionError(err) {
			return nil, err
		}
		users.Skip(scrapper.PlatformUserFactRoles, "ACCOUNT_USAGE.GRANTS_TO_USERS was refused: "+err.Error())
	} else {
		users.AssignRoles(roles)
	}
	return users, nil
}

func (e *SnowflakeScrapper) queryAccountUsageUserRoles(ctx context.Context) (map[string][]string, error) {
	rows, err := e.executor.QueryRows(ctx, fmt.Sprintf(accountUsageUserRolesQuery, e.accountUsageDb()))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	roleRows, err := scrapper.ScanAll[userRoleRow](ctx, rows, "ACCOUNT_USAGE.GRANTS_TO_USERS")
	if err != nil {
		return nil, err
	}
	roles := map[string][]string{}
	for _, r := range roleRows {
		roles[r.GranteeName] = append(roles[r.GranteeName], r.Role)
	}
	return roles, nil
}

func (e *SnowflakeScrapper) queryShowUsers(ctx context.Context) (*scrapper.PlatformUserListing, error) {
	var all []*showUsersRow
	after := ""
	for {
		query := fmt.Sprintf("SHOW USERS LIMIT %d", showUsersPageSize)
		if after != "" {
			query += " FROM " + e.SqlDialect().StringLiteral(after)
		}
		page, err := e.showUsersPage(ctx, query)
		if err != nil {
			return nil, err
		}
		all = append(all, page...)
		if len(page) < showUsersPageSize {
			break
		}
		after = page[len(page)-1].Name
	}
	return platformUsersFromShowUsers(all, time.Now()), nil
}

func (e *SnowflakeScrapper) showUsersPage(ctx context.Context, query string) ([]*showUsersRow, error) {
	rows, err := e.executor.QueryRows(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scrapper.ScanAll[showUsersRow](ctx, rows, "SHOW USERS")
}

// platformUsersFromShowUsers maps SHOW USERS rows. Every user is named, but a
// user the role may not see the details of comes back with a NULL disabled
// column, which no visible user has, and so do all its other details. Those
// facts are recorded as skipped when any user hides them, since the caller
// cannot tell a hidden email from a missing one otherwise.
func platformUsersFromShowUsers(rows []*showUsersRow, now time.Time) *scrapper.PlatformUserListing {
	users := &scrapper.PlatformUserListing{
		Source:       showUsersSource,
		Kind:         scrapper.PlatformUserSourceSQL,
		Completeness: scrapper.PlatformUsersUnknown,
		CompletenessReason: "Snowflake does not say whether SHOW USERS names every user to a role without MANAGE GRANTS; " +
			"ACCOUNT_USAGE.USERS is the account's own record of every user, read with " + platformUsersGrant,
	}
	hidden := 0
	for _, r := range rows {
		if !r.Disabled.Valid {
			hidden++
		}
		users.Users = append(users.Users, &scrapper.PlatformUser{
			Login:       r.Name,
			Type:        r.Type.String,
			Email:       r.Email.String,
			DisplayName: displayName(r.DisplayName, r.FirstName, r.LastName, r.Name),
			Comment:     r.Comment.String,
			Disabled:    isDisabled(parseShowBool(r.Disabled), parseShowBool(r.SnowflakeLock), r.ExpiresAt, now),
			CreatedAt:   utcTime(r.CreatedOn),
			LastLoginAt: utcTime(r.LastSuccessLogin),
			DefaultRole: r.DefaultRole.String,
		})
	}

	users.Skip(scrapper.PlatformUserFactPlatformId, "SHOW USERS does not return USER_ID")
	users.Skip(scrapper.PlatformUserFactRoles, "SHOW USERS does not return the roles granted to a user")
	if hidden > 0 {
		reason := fmt.Sprintf("SHOW USERS hides it for the %d of %d users the role does not own; grant MANAGE GRANTS, or %s",
			hidden, len(rows), platformUsersGrant)
		for _, fact := range []scrapper.PlatformUserFact{
			scrapper.PlatformUserFactType,
			scrapper.PlatformUserFactEmail,
			scrapper.PlatformUserFactDisplayName,
			scrapper.PlatformUserFactComment,
			scrapper.PlatformUserFactDisabled,
			scrapper.PlatformUserFactDefaultRole,
		} {
			users.Skip(fact, reason)
		}
	}
	return users
}

// displayName prefers DISPLAY_NAME, then FIRST_NAME LAST_NAME. Snowflake
// defaults DISPLAY_NAME to the user's name, which says nothing more, so a
// display name equal to the login is dropped.
func displayName(display, first, last sql.NullString, login string) string {
	name := strings.TrimSpace(display.String)
	if name == "" {
		name = strings.TrimSpace(strings.TrimSpace(first.String) + " " + strings.TrimSpace(last.String))
	}
	if name == login {
		return ""
	}
	return name
}

// isDisabled reports whether the user cannot sign in: disabled by an
// administrator, locked by Snowflake, or past its expiry. Nil when the
// DISABLED column itself is unknown.
func isDisabled(disabled, locked sql.NullBool, expiresAt sql.NullTime, now time.Time) *bool {
	if !disabled.Valid {
		return nil
	}
	v := disabled.Bool || (locked.Valid && locked.Bool) || (expiresAt.Valid && expiresAt.Time.Before(now))
	return &v
}

func parseShowBool(s sql.NullString) sql.NullBool {
	if !s.Valid {
		return sql.NullBool{}
	}
	switch strings.ToLower(strings.TrimSpace(s.String)) {
	case "true":
		return sql.NullBool{Bool: true, Valid: true}
	case "false":
		return sql.NullBool{Bool: false, Valid: true}
	}
	return sql.NullBool{}
}

func utcTime(t sql.NullTime) *time.Time {
	if !t.Valid {
		return nil
	}
	v := t.Time.UTC()
	return &v
}
