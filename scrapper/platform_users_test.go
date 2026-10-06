package scrapper

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPlatformUsersFinish(t *testing.T) {
	created := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	yes := true

	t.Run("sorts users and roles, drops empty logins and duplicate roles", func(t *testing.T) {
		users := (&PlatformUsers{
			Completeness: PlatformUsersComplete,
			Users: []*PlatformUser{
				{Login: "ZED", Roles: []string{"B", "A", "B", ""}},
				nil,
				{Login: "  "},
				{Login: ""},
				{Login: "ALICE"},
			},
		}).Finish()

		require.Len(t, users.Users, 2)
		assert.Equal(t, "ALICE", users.Users[0].Login)
		assert.Nil(t, users.Users[0].Roles)
		assert.Equal(t, "ZED", users.Users[1].Login)
		assert.Equal(t, []string{"A", "B"}, users.Users[1].Roles)
		assert.Equal(t, PlatformUsersComplete, users.Completeness)
	})

	t.Run("merges users listed twice, first value wins, roles united", func(t *testing.T) {
		users := (&PlatformUsers{Users: []*PlatformUser{
			{Login: "SVC", Email: "first@example.com", Roles: []string{"LOADER"}},
			{Login: "SVC", Email: "second@example.com", Type: "SERVICE", Disabled: &yes, CreatedAt: &created, Roles: []string{"READER", "LOADER"}},
		}}).Finish()

		require.Len(t, users.Users, 1)
		u := users.Users[0]
		assert.Equal(t, "first@example.com", u.Email)
		assert.Equal(t, "SERVICE", u.Type)
		assert.Equal(t, &yes, u.Disabled)
		assert.Equal(t, &created, u.CreatedAt)
		assert.Equal(t, []string{"LOADER", "READER"}, u.Roles)
	})

	t.Run("logins differing only in case stay apart", func(t *testing.T) {
		// Quoted identifiers make "jdoe" and "JDOE" two logins on Snowflake and Postgres.
		users := (&PlatformUsers{Users: []*PlatformUser{{Login: "jdoe"}, {Login: "JDOE"}}}).Finish()
		assert.Len(t, users.Users, 2)
	})

	t.Run("an empty listing is marked empty whatever was claimed", func(t *testing.T) {
		users := (&PlatformUsers{Completeness: PlatformUsersComplete}).Finish()
		assert.Equal(t, PlatformUsersEmpty, users.Completeness)
		assert.NotEmpty(t, users.CompletenessReason)
		assert.Empty(t, users.Users)

		users = (&PlatformUsers{Completeness: PlatformUsersLimited, CompletenessReason: "role owns no users"}).Finish()
		assert.Equal(t, PlatformUsersEmpty, users.Completeness)
		assert.Equal(t, "role owns no users", users.CompletenessReason)
	})

	t.Run("unset completeness is unknown", func(t *testing.T) {
		users := (&PlatformUsers{Users: []*PlatformUser{{Login: "A"}}}).Finish()
		assert.Equal(t, PlatformUsersUnknown, users.Completeness)
	})

	t.Run("skipped facts are sorted and recorded once", func(t *testing.T) {
		users := &PlatformUsers{Users: []*PlatformUser{{Login: "A"}}}
		users.Skip(PlatformUserFactRoles, "refused")
		users.Skip(PlatformUserFactEmail, "not on this platform")
		users.Skip(PlatformUserFactRoles, "second reason")
		users.Finish()

		assert.Equal(t, []SkippedPlatformUserFact{
			{Fact: PlatformUserFactEmail, Reason: "not on this platform"},
			{Fact: PlatformUserFactRoles, Reason: "refused"},
		}, users.SkippedFacts)
		assert.True(t, users.IsSkipped(PlatformUserFactRoles))
		assert.False(t, users.IsSkipped(PlatformUserFactType))
	})
}

func TestPlatformUsersAssignRoles(t *testing.T) {
	users := &PlatformUsers{Users: []*PlatformUser{{Login: "A", Roles: []string{"X"}}, {Login: "B"}}}
	users.AssignRoles(map[string][]string{"A": {"Y"}, "GHOST": {"Z"}})
	users.Finish()

	require.Len(t, users.Users, 2, "a grant to an unlisted login adds no user")
	assert.Equal(t, []string{"X", "Y"}, users.Users[0].Roles)
	assert.Nil(t, users.Users[1].Roles)
}

func TestPlatformUsersSanitize(t *testing.T) {
	users := &PlatformUsers{
		CompletenessReason: "bad\x00reason",
		SkippedFacts:       []SkippedPlatformUserFact{{Fact: PlatformUserFactEmail, Reason: "r\xff"}},
		Users: []*PlatformUser{{
			Login: "A", Email: "a\x00@example.com", DisplayName: "J\xffD", Comment: "c\x00",
			Type: "T\x00", PlatformId: "1\x00", DefaultRole: "R\x00", Roles: []string{"X\x00"},
		}},
	}
	users.Sanitize()

	assert.Equal(t, "badreason", users.CompletenessReason)
	assert.Equal(t, "r", users.SkippedFacts[0].Reason)
	u := users.Users[0]
	assert.Equal(t, "a@example.com", u.Email)
	assert.Equal(t, "JD", u.DisplayName)
	assert.Equal(t, "c", u.Comment)
	assert.Equal(t, "T", u.Type)
	assert.Equal(t, "1", u.PlatformId)
	assert.Equal(t, "R", u.DefaultRole)
	assert.Equal(t, []string{"X"}, u.Roles)

	var nilUsers *PlatformUsers
	nilUsers.Sanitize()
}

func TestPlatformUserHasValidIdentity(t *testing.T) {
	assert.True(t, (&PlatformUser{Login: "jdoe@example.com"}).HasValidIdentity())
	assert.False(t, (&PlatformUser{}).HasValidIdentity())
	assert.False(t, (&PlatformUser{Login: "j\x00doe"}).HasValidIdentity())
	assert.False(t, (&PlatformUser{Login: "j\xffdoe"}).HasValidIdentity())
	var nilUser *PlatformUser
	assert.False(t, nilUser.HasValidIdentity())
}
