package db2

import (
	"strings"

	godb2 "github.com/go-db2/go-db2"
	"github.com/pkg/errors"
)

// IsAuthError reports whether err is the server's security check rejecting the
// login. go-db2 reports a failed SECCHK during the handshake as text only, with
// its SECCHKCD, so that text is matched as well as the typed forms.
func IsAuthError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, godb2.ErrAuthenticationFailed) {
		return true
	}
	var db2Err *godb2.Db2Error
	if errors.As(err, &db2Err) && db2Err.SQLCode == -30082 {
		return true
	}
	return strings.Contains(err.Error(), "authentication failed: SECCHKCD=")
}
