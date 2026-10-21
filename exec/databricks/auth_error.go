package databricks

import (
	"strings"

	"github.com/databricks/databricks-sdk-go/apierr"
	"github.com/pkg/errors"
	"golang.org/x/oauth2"
)

// IsAuthError reports whether err is the workspace rejecting the credentials:
// a 401, the 403 it answers an unknown or revoked personal access token with
// ("Invalid access token"), or the OAuth token endpoint refusing a service
// principal's client id and secret.
//
// The SDK's "cannot configure default credentials" is not one. It is what the
// SDK says when no credential strategy got as far as producing a token, and an
// OAuth strategy gets no further than its endpoint discovery when the host does
// not resolve, so it is as often an unreachable workspace as a wrong secret.
func IsAuthError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, apierr.ErrUnauthenticated) {
		return true
	}
	if errors.Is(err, apierr.ErrPermissionDenied) && strings.Contains(strings.ToLower(err.Error()), "invalid access token") {
		return true
	}
	var retrieveErr *oauth2.RetrieveError
	if errors.As(err, &retrieveErr) && retrieveErr.Response != nil &&
		retrieveErr.Response.StatusCode >= 400 && retrieveErr.Response.StatusCode < 500 {
		return true
	}
	return strings.Contains(err.Error(), "invalid_client")
}
