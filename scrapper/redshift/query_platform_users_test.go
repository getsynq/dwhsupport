package redshift

import (
	"testing"

	"github.com/lib/pq"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestPlatformUsersSourceError(t *testing.T) {
	refused := platformUsersSourceError(errors.Wrap(&pq.Error{Code: "42501", Message: "permission denied for relation pg_user"}, "query"))
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
	assert.Contains(t, platformUserFactSkipReason("SYS_CONNECTION_LOG", &pq.Error{Code: "42501", Message: "denied"}), "refused")
	assert.Contains(t, platformUserFactSkipReason("SVV_USER_GRANTS", &pq.Error{Code: "42P01", Message: "does not exist"}),
		"not available on this cluster")
	assert.Contains(t, platformUserFactSkipReason("PG_GROUP", errors.New("i/o timeout")), "failed")
}

func TestWhyNotOthers(t *testing.T) {
	assert.Empty(t, whyNotOthers(true, nil))
	assert.Contains(t, whyNotOthers(false, nil), "ACCESS SYSTEM TABLE")

	// A cluster from before RBAC has no has_system_privilege: the reason says
	// it could not tell, and that the function is not on this cluster.
	why := whyNotOthers(false, &pq.Error{Code: "42883", Message: "function has_system_privilege does not exist"})
	assert.Contains(t, why, "could not tell")
	assert.Contains(t, why, "not available on this cluster")
}
