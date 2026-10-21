package trino

import (
	"net"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/trinodb/trino-go-client/trino"
)

func TestIsAuthError(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"unauthorized", &trino.ErrQueryFailed{StatusCode: 401, Reason: errors.New("Invalid credentials")}, true},
		{"refused connection", &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connect: connection refused")}, false},
		{"forbidden", &trino.ErrQueryFailed{StatusCode: 403, Reason: errors.New("Access Denied")}, false},
		{"nil", nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, IsAuthError(tc.err))
		})
	}
}
