package databricks

import (
	"net"
	"net/http"
	"testing"

	"github.com/databricks/databricks-sdk-go/apierr"
	"github.com/databricks/databricks-sdk-go/config"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"golang.org/x/oauth2"
)

func TestIsAuthError(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{
			"unauthenticated",
			&apierr.APIError{StatusCode: http.StatusUnauthorized, ErrorCode: "UNAUTHENTICATED", Message: "Credential was not sent"},
			true,
		},
		{
			"invalid personal access token",
			&apierr.APIError{StatusCode: http.StatusForbidden, ErrorCode: "403", Message: "Invalid access token."},
			true,
		},
		{
			"client secret refused",
			errors.Wrap(
				&oauth2.RetrieveError{Response: &http.Response{StatusCode: http.StatusUnauthorized}, ErrorCode: "invalid_client"},
				"inner token",
			),
			true,
		},
		{"no such host", &net.DNSError{Err: "no such host", Name: "dbc.example.com", IsNotFound: true}, false},
		{
			"missing grant",
			&apierr.APIError{StatusCode: http.StatusForbidden, ErrorCode: "PERMISSION_DENIED", Message: "User is not authorized"},
			false,
		},
		// What an OAuth integration gets when the host does not resolve, as well as
		// when nothing is configured.
		{"no credential strategy", errors.Wrap(config.ErrCannotConfigureDefault, "default auth"), false},
		{"nil", nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, IsAuthError(tc.err))
		})
	}
}
