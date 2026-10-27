package bigquery

import (
	"net/http"

	"cloud.google.com/go/auth"
	"github.com/pkg/errors"
	"golang.org/x/oauth2"
	"google.golang.org/api/googleapi"
)

// IsAuthError reports whether err is Google rejecting the credentials: the
// token endpoint refusing a service account key or a refresh token (invalid_grant
// and the like, any 4xx), or the API refusing the token it was sent (401).
//
// A denied impersonation hop (see ImpersonationError.Denied) is one too: Google refused to mint a
// token for the identity the connection names.
//
// A 403 from the API is not one: the identity was accepted and lacks a grant,
// which IsPermissionError reports.
func IsAuthError(err error) bool {
	if impErr, ok := AsImpersonationError(err); ok {
		return impErr.Denied()
	}
	var authErr *auth.Error
	if errors.As(err, &authErr) && authErr.Response != nil && isClientError(authErr.Response.StatusCode) {
		return true
	}
	var retrieveErr *oauth2.RetrieveError
	if errors.As(err, &retrieveErr) && retrieveErr.Response != nil && isClientError(retrieveErr.Response.StatusCode) {
		return true
	}
	var apiErr *googleapi.Error
	return errors.As(err, &apiErr) && apiErr.Code == http.StatusUnauthorized
}

func isClientError(status int) bool {
	return status >= 400 && status < 500
}
