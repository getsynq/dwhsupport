package snowflake

import (
	"database/sql"
	"testing"
	"time"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func str(s string) sql.NullString { return sql.NullString{String: s, Valid: true} }

func TestPlatformUsersFromShowUsers(t *testing.T) {
	now := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	created := time.Date(2025, 1, 2, 3, 4, 5, 0, time.FixedZone("PDT", -7*3600))

	t.Run("a role that owns every user sees every fact", func(t *testing.T) {
		users := platformUsersFromShowUsers([]*showUsersRow{{
			Name:        "LOADER",
			Type:        str("SERVICE"),
			Email:       str("loader@example.com"),
			DisplayName: str("LOADER"),
			Comment:     str("used by the loader"),
			Disabled:    str("false"),
			DefaultRole: str("LOADER_ROLE"),
			CreatedOn:   sql.NullTime{Time: created, Valid: true},
		}}, now).Finish()

		require.Len(t, users.Users, 1)
		u := users.Users[0]
		assert.Equal(t, "SERVICE", u.Type)
		assert.Equal(t, "loader@example.com", u.Email)
		assert.Empty(t, u.DisplayName, "a display name equal to the login says nothing more")
		assert.Equal(t, "used by the loader", u.Comment)
		require.NotNil(t, u.Disabled)
		assert.False(t, *u.Disabled)
		assert.Equal(t, "LOADER_ROLE", u.DefaultRole)
		assert.Equal(t, created.UTC(), *u.CreatedAt)
		assert.Equal(t, time.UTC, u.CreatedAt.Location())

		assert.Equal(t, scrapper.PlatformUsersUnknown, users.Completeness)
		assert.Contains(t, users.CompletenessReason, "IMPORTED PRIVILEGES")
		assert.True(t, users.IsSkipped(scrapper.PlatformUserFactRoles))
		assert.True(t, users.IsSkipped(scrapper.PlatformUserFactPlatformId))
		assert.False(t, users.IsSkipped(scrapper.PlatformUserFactEmail))
	})

	t.Run("users the role does not own hide their facts", func(t *testing.T) {
		users := platformUsersFromShowUsers([]*showUsersRow{
			{Name: "OWNED", Disabled: str("true"), Email: str("o@example.com")},
			{Name: "OTHER", CreatedOn: sql.NullTime{Time: created, Valid: true}},
		}, now).Finish()

		require.Len(t, users.Users, 2)
		other, owned := users.Users[0], users.Users[1]
		assert.Equal(t, "OTHER", other.Login)
		assert.Nil(t, other.Disabled)
		assert.NotNil(t, other.CreatedAt)
		require.NotNil(t, owned.Disabled)
		assert.True(t, *owned.Disabled)

		for _, fact := range []scrapper.PlatformUserFact{
			scrapper.PlatformUserFactEmail, scrapper.PlatformUserFactType, scrapper.PlatformUserFactDisabled,
			scrapper.PlatformUserFactDefaultRole, scrapper.PlatformUserFactComment, scrapper.PlatformUserFactDisplayName,
		} {
			assert.Truef(t, users.IsSkipped(fact), "%s is hidden for OTHER", fact)
		}
		for _, f := range users.SkippedFacts {
			if f.Fact == scrapper.PlatformUserFactEmail {
				assert.Contains(t, f.Reason, "1 of 2 users")
				assert.Contains(t, f.Reason, "MANAGE GRANTS")
			}
		}
	})

	t.Run("no users is empty, not complete", func(t *testing.T) {
		users := platformUsersFromShowUsers(nil, now).Finish()
		assert.Equal(t, scrapper.PlatformUsersEmpty, users.Completeness)
	})
}

func TestIsDisabled(t *testing.T) {
	now := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	yes := sql.NullBool{Bool: true, Valid: true}
	no := sql.NullBool{Bool: false, Valid: true}
	past := sql.NullTime{Time: now.Add(-time.Hour), Valid: true}
	future := sql.NullTime{Time: now.Add(time.Hour), Valid: true}

	for _, tc := range []struct {
		name     string
		disabled sql.NullBool
		locked   sql.NullBool
		expires  sql.NullTime
		want     *bool
	}{
		{"unknown", sql.NullBool{}, yes, past, nil},
		{"enabled", no, no, future, boolPtr(false)},
		{"enabled, lock unknown", no, sql.NullBool{}, sql.NullTime{}, boolPtr(false)},
		{"disabled", yes, no, sql.NullTime{}, boolPtr(true)},
		{"locked by snowflake", no, yes, sql.NullTime{}, boolPtr(true)},
		{"expired", no, no, past, boolPtr(true)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, isDisabled(tc.disabled, tc.locked, tc.expires, now))
		})
	}
}

func TestParseShowBool(t *testing.T) {
	assert.Equal(t, sql.NullBool{Bool: true, Valid: true}, parseShowBool(str("true")))
	assert.Equal(t, sql.NullBool{Bool: false, Valid: true}, parseShowBool(str("FALSE")))
	assert.Equal(t, sql.NullBool{}, parseShowBool(str("")))
	assert.Equal(t, sql.NullBool{}, parseShowBool(sql.NullString{}))
}

func TestDisplayName(t *testing.T) {
	assert.Equal(t, "Jane Doe", displayName(str("Jane Doe"), str("X"), str("Y"), "JDOE"))
	assert.Equal(t, "Jane Doe", displayName(sql.NullString{}, str("Jane"), str("Doe"), "JDOE"))
	assert.Equal(t, "Jane", displayName(str(" "), str("Jane"), sql.NullString{}, "JDOE"))
	assert.Empty(t, displayName(str("JDOE"), sql.NullString{}, sql.NullString{}, "JDOE"))
	assert.Empty(t, displayName(sql.NullString{}, sql.NullString{}, sql.NullString{}, "JDOE"))
}

func boolPtr(b bool) *bool { return &b }
