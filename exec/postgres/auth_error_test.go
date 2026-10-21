package postgres

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
		{"role not let in", errors.Wrap(&pq.Error{Code: "28000", Message: `no pg_hba.conf entry for host`}, "ping"), true},
		{"refused connection", &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connect: connection refused")}, false},
		{"missing grant", &pq.Error{Code: "42501", Message: "permission denied for table t"}, false},
		{"database does not exist", &pq.Error{Code: "3D000", Message: `database "d" does not exist`}, false},
		{"nil", nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, IsAuthError(tc.err))
		})
	}
}
