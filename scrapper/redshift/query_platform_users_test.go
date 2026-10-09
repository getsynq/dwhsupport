package redshift

import (
	"testing"

	"github.com/getsynq/dwhsupport/scrapper"
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

func TestPlatformUserFactSkip(t *testing.T) {
	kind, reason := platformUserFactSkip("SYS_CONNECTION_LOG", &pq.Error{Code: "42501", Message: "denied"})
	assert.Equal(t, scrapper.PlatformUserSkipRefused, kind)
	assert.Contains(t, reason, "refused")

	kind, reason = platformUserFactSkip("SVV_USER_GRANTS", &pq.Error{Code: "42P01", Message: "does not exist"})
	assert.Equal(t, scrapper.PlatformUserSkipUnavailable, kind)
	assert.Contains(t, reason, "not available on this cluster")

	kind, reason = platformUserFactSkip("PG_GROUP", errors.New("i/o timeout"))
	assert.Equal(t, scrapper.PlatformUserSkipFailed, kind)
	assert.Contains(t, reason, "failed")
}

func TestPlatformUserFactsRedshiftDoesNotKeepAreUnavailable(t *testing.T) {
	for _, skipped := range skippedPlatformUserFacts {
		assert.Equalf(t, scrapper.PlatformUserSkipUnavailable, skipped.Kind, "%s", skipped.Fact)
	}
}

func TestWhyNotOthers(t *testing.T) {
	kind, why := whyNotOthers(true, nil)
	assert.Empty(t, kind)
	assert.Empty(t, why)

	// Without ACCESS SYSTEM TABLE a grant would let the fact be read.
	kind, why = whyNotOthers(false, nil)
	assert.Equal(t, scrapper.PlatformUserSkipRefused, kind)
	assert.Contains(t, why, "ACCESS SYSTEM TABLE")

	// A cluster from before RBAC has no has_system_privilege: the reason says
	// it could not tell, and that the function is not on this cluster.
	kind, why = whyNotOthers(false, &pq.Error{Code: "42883", Message: "function has_system_privilege does not exist"})
	assert.Equal(t, scrapper.PlatformUserSkipUnavailable, kind)
	assert.Contains(t, why, "could not tell")
	assert.Contains(t, why, "not available on this cluster")

	kind, _ = whyNotOthers(false, errors.New("i/o timeout"))
	assert.Equal(t, scrapper.PlatformUserSkipFailed, kind)
}
