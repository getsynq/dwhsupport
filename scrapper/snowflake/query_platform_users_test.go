package snowflake

import (
	"database/sql"
	"testing"
	"time"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
	"github.com/pkg/errors"
	gosnowflake "github.com/snowflakedb/gosnowflake"
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
		}}, now, scrapper.PlatformUserSkipUnavailable).Finish()

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
		assert.Equal(t, showUsersSource, users.Source)
		assert.Equal(t, scrapper.PlatformUserSourceSQL, users.Kind)
		for _, fact := range []scrapper.PlatformUserFact{scrapper.PlatformUserFactRoles, scrapper.PlatformUserFactPlatformId} {
			scrappertest.AssertSkipped(t, users, fact, scrapper.PlatformUserSkipUnavailable)
		}
		assert.False(t, users.IsSkipped(scrapper.PlatformUserFactEmail))
	})

	t.Run("the facts only ACCOUNT_USAGE states are skipped the way it did not answer", func(t *testing.T) {
		for _, kind := range []scrapper.PlatformUserSkipKind{
			scrapper.PlatformUserSkipRefused, scrapper.PlatformUserSkipFailed, scrapper.PlatformUserSkipUnavailable,
		} {
			users := platformUsersFromShowUsers([]*showUsersRow{{Name: "LOADER", Disabled: str("false")}}, now, kind).Finish()
			scrappertest.AssertSkipped(t, users, scrapper.PlatformUserFactRoles, kind)
			scrappertest.AssertSkipped(t, users, scrapper.PlatformUserFactPlatformId, kind)
		}
	})

	t.Run("users the role does not own hide their facts", func(t *testing.T) {
		users := platformUsersFromShowUsers([]*showUsersRow{
			{Name: "OWNED", Disabled: str("true"), Email: str("o@example.com")},
			{Name: "OTHER", CreatedOn: sql.NullTime{Time: created, Valid: true}},
		}, now, scrapper.PlatformUserSkipUnavailable).Finish()

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
			skip, ok := users.SkippedFact(fact)
			assert.Truef(t, ok, "%s is hidden for OTHER", fact)
			assert.Equalf(t, scrapper.PlatformUserSkipRefused, skip.Kind, "a grant shows %s of OTHER", fact)
		}
		for _, f := range users.SkippedFacts {
			if f.Fact == scrapper.PlatformUserFactEmail {
				assert.Contains(t, f.Reason, "1 of 2 users")
				assert.Contains(t, f.Reason, "MANAGE GRANTS")
			}
		}
	})

	t.Run("no users is empty, not complete", func(t *testing.T) {
		users := platformUsersFromShowUsers(nil, now, scrapper.PlatformUserSkipUnavailable).Finish()
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

func TestPlatformUserSourceErrorClassification(t *testing.T) {
	classify := func(err error) *scrapper.PlatformUserListing {
		return scrapper.PlatformUserSourceError("s", scrapper.PlatformUserSourceSQL, err, isRefused, isUnavailable)
	}

	assert.NotEmpty(t, classify(&gosnowflake.SnowflakeError{Number: 904, Message: "invalid identifier 'TYPE'"}).Unavailable,
		"a column this account lacks is no grant's business")
	assert.NotEmpty(t, classify(&gosnowflake.SnowflakeError{Number: 2003, Message: "Object does not exist or not authorized."}).Refused)
	assert.NotEmpty(t, classify(errors.New("i/o timeout")).Failed)

	// A resource monitor over its quota stops the warehouse, which no grant on
	// the users views fixes and a later run may get past: failed, not refused.
	quota := &gosnowflake.SnowflakeError{
		Number:  90073,
		Message: "Warehouse 'MY_WH' cannot be resumed because resource monitor 'MY_RM' has exceeded its quota.",
	}
	assert.NotEmpty(t, classify(quota).Failed, "refused=%q", classify(quota).Refused)
	assert.NotEmpty(t, classify(errors.Wrap(errors.New(quota.Message), "query")).Failed, "matched by its message too")
}

func TestPlatformUsersRolesSkip(t *testing.T) {
	for _, tc := range []struct {
		name   string
		err    error
		kind   scrapper.PlatformUserSkipKind
		reason string
	}{
		{
			"refused", &gosnowflake.SnowflakeError{Number: 2003, Message: "Object does not exist or not authorized."},
			scrapper.PlatformUserSkipRefused, "was refused",
		},
		{
			"insufficient privileges", &gosnowflake.SnowflakeError{Number: 3001, Message: "Insufficient privileges"},
			scrapper.PlatformUserSkipRefused, "was refused",
		},
		{
			"resource monitor quota", &gosnowflake.SnowflakeError{Number: 90073, Message: "cannot be resumed because resource monitor"},
			scrapper.PlatformUserSkipFailed, "failed",
		},
		{
			"missing column", &gosnowflake.SnowflakeError{Number: 904, Message: "invalid identifier 'ROLE'"},
			scrapper.PlatformUserSkipUnavailable, "cannot read",
		},
		{"timeout", errors.New("i/o timeout"), scrapper.PlatformUserSkipFailed, "failed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			kind, reason := rolesSkip(tc.err)
			assert.Equal(t, tc.kind, kind)
			assert.Contains(t, reason, "GRANTS_TO_USERS")
			assert.Contains(t, reason, tc.reason)
		})
	}
}
