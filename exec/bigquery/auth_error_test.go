package bigquery

import (
	"net"
	"net/http"
	"testing"

	"cloud.google.com/go/auth"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"golang.org/x/oauth2"
	"google.golang.org/api/googleapi"
)

func TestIsAuthError(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{
			"token refused by the API",
			&googleapi.Error{Code: http.StatusUnauthorized, Message: "Request had invalid authentication credentials."},
			true,
		},
		{"key refused by the token endpoint", errors.Wrap(&auth.Error{Response: &http.Response{StatusCode: http.StatusBadRequest}}, "ping"), true},
		{
			"refresh token refused",
			&oauth2.RetrieveError{Response: &http.Response{StatusCode: http.StatusBadRequest}, ErrorCode: "invalid_grant"},
			true,
		},
		{"token endpoint down", &auth.Error{Response: &http.Response{StatusCode: http.StatusServiceUnavailable}}, false},
		{"no such host", &net.DNSError{Err: "no such host", Name: "bigquery.googleapis.com", IsNotFound: true}, false},
		{"missing grant", &googleapi.Error{Code: http.StatusForbidden, Message: "Access Denied"}, false},
		{"nil", nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, IsAuthError(tc.err))
		})
	}
}
