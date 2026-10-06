package postgres

import (
	"testing"

	"github.com/lib/pq"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestPlatformUsersSourceError(t *testing.T) {
	refused := platformUsersSourceError(errors.Wrap(&pq.Error{Code: "42501", Message: "permission denied for view pg_roles"}, "query"))
	assert.NotEmpty(t, refused.Refused)
	assert.Empty(t, refused.Unavailable)
	assert.Empty(t, refused.Failed)
	assert.Equal(t, platformUsersSource, refused.Source)

	for _, code := range []pq.ErrorCode{"42P01", "42703", "42883", "0A000"} {
		unavailable := platformUsersSourceError(&pq.Error{Code: code, Message: "missing"})
		assert.NotEmptyf(t, unavailable.Unavailable, "code %s", code)
		assert.Emptyf(t, unavailable.Refused, "code %s", code)
		assert.Emptyf(t, unavailable.Failed, "code %s", code)
	}

	failed := platformUsersSourceError(errors.New("connection reset by peer"))
	assert.NotEmpty(t, failed.Failed)
	assert.Empty(t, failed.Refused)
	assert.Empty(t, failed.Unavailable)
	assert.False(t, failed.Answered())
}

func TestPlatformUserFactSkipReason(t *testing.T) {
	assert.Contains(t, platformUserFactSkipReason("pg_auth_members", &pq.Error{Code: "42501", Message: "denied"}), "refused")
	assert.Contains(t, platformUserFactSkipReason("shobj_description", &pq.Error{Code: "42883", Message: "no such function"}),
		"not available on this server")
	assert.Contains(t, platformUserFactSkipReason("pg_roles.rolconfig", errors.New("i/o timeout")), "failed")
}
