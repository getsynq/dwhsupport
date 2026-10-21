package mssql

import (
	"testing"

	mssql "github.com/microsoft/go-mssqldb"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestIsAuthError(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"login failed", mssql.Error{Number: 18456, Message: "Login failed for user 'u'."}, true},
		{
			"password expired",
			errors.Wrap(mssql.Error{Number: 18487, Message: "Login failed for user 'u'. Reason: The password of the account has expired."}, "ping"),
			true,
		},
		{
			"TLS handshake",
			errors.New("TLS Handshake failed: tls: failed to parse certificate from server: x509: negative serial number"),
			false,
		},
		{"database cannot be opened", mssql.Error{Number: 4060, Message: `Cannot open database "d" requested by the login.`}, false},
		{"missing grant", mssql.Error{Number: 229, Message: "The SELECT permission was denied on the object 't'"}, false},
		{"nil", nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, IsAuthError(tc.err))
		})
	}
}
