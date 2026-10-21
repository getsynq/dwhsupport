package mysql

import (
	"net"
	"testing"

	"github.com/go-sql-driver/mysql"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestIsAuthError(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"wrong password", &mysql.MySQLError{Number: 1045, Message: "Access denied for user 'u'@'h' (using password: YES)"}, true},
		{"account locked", errors.Wrap(&mysql.MySQLError{Number: 3118, Message: "Access denied for user 'u'@'h'. Account is locked."}, "ping"), true},
		{"unsupported auth method", mysql.ErrCleartextPassword, true},
		{"read EOF", errors.New("connection error: read: EOF"), false},
		{"refused connection", &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connect: connection refused")}, false},
		{"database not granted", &mysql.MySQLError{Number: 1044, Message: "Access denied for user 'u'@'%' to database 'd'"}, false},
		{"nil", nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, IsAuthError(tc.err))
		})
	}
}
