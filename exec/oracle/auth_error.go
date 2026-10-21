package oracle

import (
	"github.com/pkg/errors"
	"github.com/sijms/go-ora/v2/network"
)

// IsAuthError reports whether err is the database rejecting the login.
func IsAuthError(err error) bool {
	var oraErr *network.OracleError
	if !errors.As(err, &oraErr) {
		return false
	}
	switch oraErr.ErrCode {
	case 1017, // invalid username/password; logon denied
		1005,  // null password given; logon denied
		28000, // the account is locked
		28001: // the password has expired
		return true
	}
	return false
}
