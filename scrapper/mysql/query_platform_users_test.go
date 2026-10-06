package mysql

import (
	"context"
	"database/sql"
	"testing"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func boolPtr(b bool) *bool { return &b }

func TestFoldAccounts(t *testing.T) {
	accounts := []*account{
		{User: "loader", Host: "10.0.0.%", Locked: boolPtr(true), Comment: "fivetran"},
		{User: "loader", Host: "%", Locked: boolPtr(false)},
		{User: "retired", Host: "%", Locked: boolPtr(true)},
		{User: "retired", Host: "localhost", Locked: boolPtr(true)},
		{User: "reporter_role", Host: "%", Locked: boolPtr(true), IsRole: true},
		{User: "mysql.sys", Host: "localhost", Locked: boolPtr(true)},
		{User: "analyst", Host: "%", DefaultRoles: []string{"reporter_role"}},
		{User: "analyst", Host: "localhost", DefaultRoles: []string{"admin_role", "reporter_role"}},
	}
	edges := []*roleEdgeRow{
		{FromUser: "reporter_role", FromHost: "%", ToUser: "analyst", ToHost: "%"},
		{FromUser: "admin_role", FromHost: "%", ToUser: "analyst", ToHost: "localhost"},
	}

	users := (&scrapper.PlatformUserListing{Users: foldAccounts(accounts, edges)}).Finish().Users
	require.Len(t, users, 3, "a role and a reserved account are not logins, hosts of one user fold")

	analyst, loader, retired := users[0], users[1], users[2]
	assert.Equal(t, "analyst", analyst.Login)
	assert.Equal(t, []string{"admin_role", "reporter_role"}, analyst.Roles)
	assert.Equal(t, "admin_role,reporter_role", analyst.DefaultRole)
	assert.Nil(t, analyst.Disabled, "no lock column read")

	assert.Equal(t, "loader", loader.Login)
	assert.Equal(t, boolPtr(false), loader.Disabled, "one unlocked host can still sign in")
	assert.Equal(t, "fivetran", loader.Comment)

	assert.Equal(t, "retired", retired.Login)
	assert.Equal(t, boolPtr(true), retired.Disabled, "every host locked")
}

func TestMarkMySQLRoles(t *testing.T) {
	accounts := []*account{
		{User: "reporter", Host: "%", Locked: boolPtr(true)},
		{User: "granted_but_unlocked", Host: "%", Locked: boolPtr(false)},
		{User: "locked_user", Host: "%", Locked: boolPtr(true)},
		{User: "analyst", Host: "%", Locked: boolPtr(false)},
	}
	markMySQLRoles(accounts,
		[]*roleEdgeRow{
			{FromUser: "reporter", FromHost: "%", ToUser: "analyst", ToHost: "%"},
			{FromUser: "granted_but_unlocked", FromHost: "%", ToUser: "analyst", ToHost: "%"},
		},
		[]*roleEdgeRow{{FromUser: "reporter", FromHost: "%", ToUser: "analyst", ToHost: "%"}},
	)
	assert.True(t, accounts[0].IsRole)
	assert.False(t, accounts[1].IsRole, "an account that can sign in is a login even when granted to others")
	assert.False(t, accounts[2].IsRole, "a locked account nobody holds is a disabled user")
	assert.Equal(t, []string{"reporter"}, accounts[3].DefaultRoles)
}

func TestMariaDBAccount(t *testing.T) {
	ctx := context.Background()
	a := mariadbAccount(ctx, &mariadbGlobalPrivRow{User: "u", Host: "%", Priv: sql.NullString{Valid: true,
		String: `{"access":0,"account_locked":true,"default_role":"reporter","password_last_changed":1}`}})
	assert.Equal(t, boolPtr(true), a.Locked)
	assert.False(t, a.IsRole)
	assert.Equal(t, []string{"reporter"}, a.DefaultRoles)

	role := mariadbAccount(ctx, &mariadbGlobalPrivRow{User: "reporter", Priv: sql.NullString{Valid: true, String: `{"is_role":true}`}})
	assert.True(t, role.IsRole)

	broken := mariadbAccount(ctx, &mariadbGlobalPrivRow{User: "u", Priv: sql.NullString{Valid: true, String: `not json`}})
	assert.Equal(t, "u", broken.User)
	assert.Nil(t, broken.Locked, "an unreadable document leaves the facts unknown")
}

func TestParseGrantee(t *testing.T) {
	for _, tc := range []struct {
		in, user, host string
		ok             bool
	}{
		{in: `'synq'@'%'`, user: "synq", host: "%", ok: true},
		{in: `'o''brien'@'10.0.0.1'`, user: "o'brien", host: "10.0.0.1", ok: true},
		{in: `'with@at'@'localhost'`, user: "with@at", host: "localhost", ok: true},
		{in: `''@'localhost'`, user: "", host: "localhost", ok: true},
		{in: `PUBLIC`, ok: false},
		{in: ``, ok: false},
	} {
		user, host, ok := parseGrantee(tc.in)
		assert.Equalf(t, tc.ok, ok, "%q", tc.in)
		assert.Equalf(t, tc.user, user, "%q", tc.in)
		assert.Equalf(t, tc.host, host, "%q", tc.in)
	}
}

func TestAttributeComment(t *testing.T) {
	assert.Equal(t, "dbt runner", attributeComment(sql.NullString{Valid: true, String: `{"comment": "dbt runner"}`}))
	assert.Equal(t, "", attributeComment(sql.NullString{}))
	assert.Equal(t, "", attributeComment(sql.NullString{Valid: true, String: `{"other": 1}`}))
	assert.Equal(t, "", attributeComment(sql.NullString{Valid: true, String: `nope`}))
}

// When mysql.role_edges cannot be read, a role (a locked account with no
// password, as CREATE ROLE makes it) that is granted to someone but is nobody's
// default must still not be listed as a user.
func TestRoleEdgesUnreadableKeepsRolesOut(t *testing.T) {
	accounts := []*account{
		{User: "reporter_role", Host: "%", Locked: boolPtr(true)},
		{User: "analyst", Host: "%", Locked: boolPtr(false)},
	}
	markMySQLRoles(accounts, nil, nil)
	users := (&scrapper.PlatformUserListing{Users: foldAccounts(accounts, nil)}).Finish().Users

	logins := []string{}
	for _, u := range users {
		logins = append(logins, u.Login)
	}
	assert.Equal(t, []string{"analyst"}, logins, "the role leaks into the listing as a disabled user")
}
