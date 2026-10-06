package mssql

import (
	"database/sql"
	"testing"
	"time"

	"github.com/getsynq/dwhsupport/scrapper"
	mssql "github.com/microsoft/go-mssqldb"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSetCompleteness(t *testing.T) {
	yes := sql.NullInt64{Valid: true, Int64: 1}
	no := sql.NullInt64{Valid: true, Int64: 0}
	null := sql.NullInt64{}

	for _, tc := range []struct {
		name      string
		v         platformUsersVisibilityRow
		want      scrapper.PlatformUsersCompleteness
		hasReason bool
	}{
		{name: "view any definition", v: platformUsersVisibilityRow{ViewAnyDefinition: yes, AlterAnyLogin: no}, want: scrapper.PlatformUsersComplete},
		{name: "alter any login", v: platformUsersVisibilityRow{ViewAnyDefinition: no, AlterAnyLogin: yes}, want: scrapper.PlatformUsersComplete},
		{name: "neither", v: platformUsersVisibilityRow{ViewAnyDefinition: no, AlterAnyLogin: no}, want: scrapper.PlatformUsersLimited, hasReason: true},
		{name: "azure sql database", v: platformUsersVisibilityRow{ViewAnyDefinition: null, AlterAnyLogin: null}, want: scrapper.PlatformUsersUnknown, hasReason: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result := &scrapper.PlatformUserListing{}
			setCompleteness(result, &tc.v)
			assert.Equal(t, tc.want, result.Completeness)
			assert.Equal(t, tc.hasReason, result.CompletenessReason != "")
		})
	}
}

func TestPlatformUsersFromRows(t *testing.T) {
	created := time.Date(2024, 3, 15, 10, 20, 30, 0, time.FixedZone("server", 3600))
	users := platformUsersFromRows([]*platformUserRow{
		{
			Login:      `DOMAIN\loader`,
			PlatformId: sql.NullString{Valid: true, String: "0x0105"},
			Type:       sql.NullString{Valid: true, String: "WINDOWS_LOGIN"},
			Disabled:   sql.NullBool{Valid: true, Bool: true},
			CreatedAt:  sql.NullTime{Valid: true, Time: created},
		},
		{Login: "contained_user", Type: sql.NullString{Valid: true, String: "SQL_USER"}},
	})
	require.Len(t, users, 2)
	assert.Equal(t, `DOMAIN\loader`, users[0].Login)
	assert.Equal(t, "0x0105", users[0].PlatformId)
	assert.Equal(t, "WINDOWS_LOGIN", users[0].Type)
	require.NotNil(t, users[0].Disabled)
	assert.True(t, *users[0].Disabled)
	require.NotNil(t, users[0].CreatedAt)
	assert.Equal(t, time.Date(2024, 3, 15, 10, 20, 30, 0, time.UTC), *users[0].CreatedAt, "the wall clock the query computed, labelled UTC")

	assert.Nil(t, users[1].Disabled, "a contained user has no login to disable")
	assert.Nil(t, users[1].CreatedAt)
}

func TestRolesByLogin(t *testing.T) {
	roles := rolesByLogin([]*platformUserRoleRow{
		{Login: "sa", Role: "sysadmin"},
		{Login: "sa", Role: "synq_test.db_owner"},
		{Login: "reader", Role: "synq_test.db_datareader"},
	})
	assert.Equal(t, map[string][]string{
		"sa":     {"sysadmin", "synq_test.db_owner"},
		"reader": {"synq_test.db_datareader"},
	}, roles)
}

func TestSourceErrorClassification(t *testing.T) {
	refused := sourceError(errors.WithStack(mssql.Error{Number: 229, Message: "The SELECT permission was denied on the object 'server_principals'"}))
	assert.NotEmpty(t, refused.Refused)

	for _, n := range []int32{207, 208} {
		unavailable := sourceError(errors.WithStack(mssql.Error{Number: n, Message: "Invalid name"}))
		assert.NotEmptyf(t, unavailable.Unavailable, "error %d", n)
	}

	failed := sourceError(errors.New("read tcp: connection reset by peer"))
	assert.NotEmpty(t, failed.Failed)
	assert.Empty(t, failed.Refused)
}

func TestFactSkipReason(t *testing.T) {
	assert.Contains(t, factSkipReason(mssql.Error{Number: 229, Message: "The SELECT permission was denied"}), "may not read role memberships; grant")
	assert.Contains(t, factSkipReason(mssql.Error{Number: 208, Message: "Invalid object name"}), "has no such role membership view")
	assert.Contains(t, factSkipReason(errors.New("i/o timeout")), "reading role memberships failed")
}
