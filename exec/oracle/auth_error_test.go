package oracle

import (
	"testing"

	"github.com/pkg/errors"
	"github.com/sijms/go-ora/v2/network"
	"github.com/stretchr/testify/assert"
)

func TestIsAuthError(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"wrong password", network.NewOracleError(1017), true},
		{"account locked", errors.Wrap(network.NewOracleError(28000), "ping"), true},
		{"connection EOF", errors.New("connection error: EOF"), false},
		{"missing grant", network.NewOracleError(1031), false},
		{"no CREATE SESSION", network.NewOracleError(1045), false},
		{"nil", nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, IsAuthError(tc.err))
		})
	}
}
