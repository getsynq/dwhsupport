package db2

import (
	"fmt"
	"testing"

	godb2 "github.com/go-db2/go-db2"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestIsAuthError(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		// The text go-db2 returns from its handshake when SECCHK is refused.
		{"handshake security check", fmt.Errorf("failed to connect to Db2 at h:50000: %w", errors.New("authentication failed: SECCHKCD=15")), true},
		{"security processing failed", &godb2.Db2Error{SQLCode: -30082, SQLState: "08001"}, true},
		{"sentinel", errors.Wrap(godb2.ErrAuthenticationFailed, "ping"), true},
		{"negotiation EOF", errors.New("ACCSEC negotiation failed: EOF"), false},
		{"missing grant", &godb2.Db2Error{SQLCode: -551, SQLState: "42501"}, false},
		{"nil", nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, IsAuthError(tc.err))
		})
	}
}
