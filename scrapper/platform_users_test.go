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
		users := (&PlatformUserListing{
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
		users := (&PlatformUserListing{Users: []*PlatformUser{
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
		users := (&PlatformUserListing{Users: []*PlatformUser{{Login: "jdoe"}, {Login: "JDOE"}}}).Finish()
		assert.Len(t, users.Users, 2)
	})

	t.Run("an empty listing is marked empty whatever was claimed", func(t *testing.T) {
		users := (&PlatformUserListing{Completeness: PlatformUsersComplete}).Finish()
		assert.Equal(t, PlatformUsersEmpty, users.Completeness)
		assert.NotEmpty(t, users.CompletenessReason)
		assert.Empty(t, users.Users)

		users = (&PlatformUserListing{Completeness: PlatformUsersLimited, CompletenessReason: "role owns no users"}).Finish()
		assert.Equal(t, PlatformUsersEmpty, users.Completeness)
		assert.Equal(t, "role owns no users", users.CompletenessReason)
	})

	t.Run("unset completeness is unknown", func(t *testing.T) {
		users := (&PlatformUserListing{Users: []*PlatformUser{{Login: "A"}}}).Finish()
		assert.Equal(t, PlatformUsersUnknown, users.Completeness)
	})

	t.Run("skipped facts are sorted and recorded once", func(t *testing.T) {
		users := &PlatformUserListing{Users: []*PlatformUser{{Login: "A"}}}
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
	users := &PlatformUserListing{Users: []*PlatformUser{{Login: "A", Roles: []string{"X"}}, {Login: "B"}}}
	users.AssignRoles(map[string][]string{"A": {"Y"}, "GHOST": {"Z"}})
	users.Finish()

	require.Len(t, users.Users, 2, "a grant to an unlisted login adds no user")
	assert.Equal(t, []string{"X", "Y"}, users.Users[0].Roles)
	assert.Nil(t, users.Users[1].Roles)
}

func TestPlatformUsersSanitize(t *testing.T) {
	users := &PlatformUserListing{
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

	var nilUsers *PlatformUserListing
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

func TestPlatformUsersReconcile(t *testing.T) {
	created := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	no := false
	yes := true
	refusal := errorString("Insufficient privileges")

	sql := func(source string, completeness PlatformUsersCompleteness, users ...*PlatformUser) *PlatformUserListing {
		return &PlatformUserListing{Source: source, Kind: PlatformUserSourceSQL, Completeness: completeness, Users: users}
	}

	t.Run("a fact comes from the first source that has it, users and roles are united", func(t *testing.T) {
		trusted := sql("p.trusted", PlatformUsersComplete,
			&PlatformUser{Login: "A", Email: "a@trusted", Disabled: &no, Roles: []string{"R1"}},
			&PlatformUser{Login: "B"})
		fresh := sql("p.fresh", PlatformUsersUnknown,
			&PlatformUser{Login: "A", Email: "a@fresh", Disabled: &yes, CreatedAt: &created, Roles: []string{"R2", "R1"}},
			&PlatformUser{Login: "NEW", Type: "SERVICE"})
		trusted.Skip(PlatformUserFactCreatedAt, "trusted has no created")

		got := NewPlatformUsers(trusted, fresh).Reconcile()

		assert.Equal(t, PlatformUsersReconciledSource, got.Source)
		require.Len(t, got.Users, 3)
		a := got.Users[0]
		assert.Equal(t, "a@trusted", a.Email)
		assert.Equal(t, &no, a.Disabled, "the trusted source's false is a value, not a gap")
		assert.Equal(t, &created, a.CreatedAt, "a gap in the trusted source is filled by the next")
		assert.Equal(t, []string{"R1", "R2"}, a.Roles)
		assert.Equal(t, "NEW", got.Users[2].Login, "a user only the fresher source names is listed")
		assert.Equal(t, PlatformUsersComplete, got.Completeness)
		assert.Empty(t, got.CompletenessReason)
		assert.False(t, got.IsSkipped(PlatformUserFactCreatedAt), "another source had it")
	})

	t.Run("reconciling leaves the sources untouched", func(t *testing.T) {
		first := sql("p.first", PlatformUsersComplete, &PlatformUser{Login: "A", Roles: []string{"R1"}})
		second := sql("p.second", PlatformUsersComplete, &PlatformUser{Login: "A", Email: "a@x", Roles: []string{"R2"}})
		result := NewPlatformUsers(first, second)
		result.Reconcile()

		assert.Empty(t, first.Users[0].Email)
		assert.Equal(t, []string{"R1"}, first.Users[0].Roles)
	})

	t.Run("a fact is skipped only when every source that answered skipped it", func(t *testing.T) {
		one := sql("p.one", PlatformUsersComplete, &PlatformUser{Login: "A"})
		one.Skip(PlatformUserFactEmail, "not here")
		one.Skip(PlatformUserFactRoles, "refused")
		two := sql("p.two", PlatformUsersUnknown, &PlatformUser{Login: "A", Roles: []string{"R"}})
		two.Skip(PlatformUserFactEmail, "not there")

		got := NewPlatformUsers(one, two, RefusedPlatformUserSource("p.three", PlatformUserSourceAPI, refusal)).Reconcile()

		assert.Equal(t, []SkippedPlatformUserFact{{Fact: PlatformUserFactEmail, Reason: "p.one: not here; p.two: not there"}}, got.SkippedFacts)
	})

	t.Run("completeness is the most complete source's, refusals explain an incomplete one", func(t *testing.T) {
		limited := sql("p.limited", PlatformUsersLimited, &PlatformUser{Login: "A"})
		limited.CompletenessReason = "sees itself only"
		unknown := sql("p.unknown", PlatformUsersUnknown, &PlatformUser{Login: "B"})
		refused := RefusedPlatformUserSource("p.api", PlatformUserSourceAPI, refusal)

		got := NewPlatformUsers(unknown, refused, limited).Reconcile()
		assert.Equal(t, PlatformUsersLimited, got.Completeness)
		assert.Equal(t, "p.limited: sees itself only; p.api refused: Insufficient privileges", got.CompletenessReason)
		assert.Len(t, got.Users, 2)

		got = NewPlatformUsers(unknown, refused).Reconcile()
		assert.Equal(t, PlatformUsersUnknown, got.Completeness)
	})

	t.Run("every source empty is empty", func(t *testing.T) {
		got := NewPlatformUsers(sql("p.a", PlatformUsersComplete), RefusedPlatformUserSource("p.b", PlatformUserSourceSQL, refusal)).Reconcile()
		assert.Equal(t, PlatformUsersEmpty, got.Completeness)
		assert.Contains(t, got.CompletenessReason, "p.b refused")
	})

	t.Run("nil and sourceless results reconcile to empty", func(t *testing.T) {
		var nilResult *PlatformUsers
		assert.Equal(t, PlatformUsersEmpty, nilResult.Reconcile().Completeness)
		assert.Equal(t, PlatformUsersEmpty, NewPlatformUsers().Reconcile().Completeness)
	})
}

func TestNewPlatformUsers(t *testing.T) {
	refused := RefusedPlatformUserSource("p.refused", PlatformUserSourceSQL, errorString("denied"))
	refused.Completeness = PlatformUsersComplete
	listed := &PlatformUserListing{Source: "p.listed", Users: []*PlatformUser{{Login: "B"}, {Login: "A"}}}

	result := NewPlatformUsers(nil, listed, refused)

	require.Len(t, result.Sources, 2)
	assert.Equal(t, "A", result.Sources[0].Users[0].Login, "each source is finished")
	assert.Equal(t, PlatformUsersUnknown, result.Sources[0].Completeness)
	assert.Empty(t, result.Sources[1].Completeness, "a refused source claims no completeness")
	assert.Equal(t, "denied", result.Sources[1].Refused)
	assert.Same(t, refused, result.Source("p.refused"))
	assert.Nil(t, result.Source("p.missing"))
	assert.False(t, result.AllRefused())
	assert.True(t, NewPlatformUsers(refused).AllRefused())
	assert.True(t, NewPlatformUsers().AllRefused(), "no source answered")
}

func TestPlatformUsersSanitizeEverySource(t *testing.T) {
	result := NewPlatformUsers(
		&PlatformUserListing{Source: "p.a", Users: []*PlatformUser{{Login: "A", Email: "a\x00@x"}}},
		RefusedPlatformUserSource("p.b", PlatformUserSourceAPI, errorString("no\xff")),
	)
	result.Sanitize()
	assert.Equal(t, "a@x", result.Sources[0].Users[0].Email)
	assert.Equal(t, "no", result.Sources[1].Refused)

	var nilResult *PlatformUsers
	nilResult.Sanitize()
}

type errorString string

func (e errorString) Error() string { return string(e) }
