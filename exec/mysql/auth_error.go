package mysql

import (
	"github.com/go-sql-driver/mysql"
	"github.com/pkg/errors"
)

// IsAuthError reports whether err is the server rejecting the login, or the
// driver refusing the authentication method the account requires.
func IsAuthError(err error) bool {
	if errors.Is(err, mysql.ErrNativePassword) ||
		errors.Is(err, mysql.ErrCleartextPassword) ||
		errors.Is(err, mysql.ErrOldPassword) ||
		errors.Is(err, mysql.ErrUnknownPlugin) {
		return true
	}
	var mysqlErr *mysql.MySQLError
	if !errors.As(err, &mysqlErr) {
		return false
	}
	switch mysqlErr.Number {
	case 1045, // ER_ACCESS_DENIED_ERROR: wrong user or password
		1251, // ER_NOT_SUPPORTED_AUTH_MODE
		1698, // ER_ACCESS_DENIED_NO_PASSWORD_ERROR
		1862, // ER_MUST_CHANGE_PASSWORD_LOGIN: the password has expired
		3118: // ER_ACCOUNT_HAS_BEEN_LOCKED
		return true
	}
	return false
}
