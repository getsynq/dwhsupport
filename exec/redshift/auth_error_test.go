package redshift

import (
	"net"
	"testing"

	"github.com/lib/pq"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestIsAuthError(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"wrong password", &pq.Error{Code: "28P01", Message: `password authentication failed for user "u"`}, true},
		{"user not let in", errors.Wrap(&pq.Error{Code: "28000", Message: "user is locked"}, "ping"), true},
		{"no such host", &net.DNSError{Err: "no such host", Name: "cluster.example.com", IsNotFound: true}, false},
		{"missing grant", &pq.Error{Code: "42501", Message: "permission denied for relation t"}, false},
		{"nil", nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, IsAuthError(tc.err))
		})
	}
}
