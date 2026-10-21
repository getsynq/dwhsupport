package mssql

import (
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	mssql "github.com/microsoft/go-mssqldb"
	"github.com/pkg/errors"
)

// IsAuthError reports whether err is the server rejecting the login, or Entra
// ID refusing to issue a token for it.
func IsAuthError(err error) bool {
	var authFailed *azidentity.AuthenticationFailedError
	if errors.As(err, &authFailed) {
		return true
	}
	var mssqlErr mssql.Error
	if !errors.As(err, &mssqlErr) {
		return false
	}
	switch mssqlErr.Number {
	case 18456, // Login failed for user
		18452, // the login is from an untrusted domain
		18470, // the account is disabled
		18486, // the account is locked out
		18487, // the password has expired
		18488: // the password must be changed
		return true
	}
	return false
}
