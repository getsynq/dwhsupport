package snowflake

import (
	"net"
	"testing"

	"github.com/pkg/errors"
	"github.com/snowflakedb/gosnowflake"
	"github.com/stretchr/testify/assert"
)

func TestIsAuthError(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"wrong password", &gosnowflake.SnowflakeError{Number: 390100, Message: "Incorrect username or password was specified."}, true},
		{"invalid JWT", errors.Wrap(&gosnowflake.SnowflakeError{Number: 390144, Message: "JWT token is invalid."}, "ping"), true},
		{"MFA required", &gosnowflake.SnowflakeError{Number: 394508, Message: "Multi-factor authentication is required"}, true},
		{"empty password", &gosnowflake.SnowflakeError{Number: gosnowflake.ErrCodeEmptyPasswordCode}, true},
		{"refused connection", &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connect: connection refused")}, false},
		{"role not granted", &gosnowflake.SnowflakeError{Number: gosnowflake.ErrRoleNotExist}, false},
		{"default database unresolvable", &gosnowflake.SnowflakeError{Number: gosnowflake.ErrObjectNotExistOrAuthorized}, false},
		{"session expired", &gosnowflake.SnowflakeError{Number: 390112}, false},
		{"statement error", &gosnowflake.SnowflakeError{Number: 2003, Message: "Object does not exist or not authorized."}, false},
		{"nil", nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, IsAuthError(tc.err))
		})
	}
}
