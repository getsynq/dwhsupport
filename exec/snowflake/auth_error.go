package snowflake

import (
	"github.com/pkg/errors"
	"github.com/snowflakedb/gosnowflake"
)

// IsAuthError reports whether err is Snowflake rejecting the login, or the
// driver refusing to try it because a credential is missing or unreadable.
//
// Snowflake answers a refused login with a code in 390xxx (390100 a wrong user
// or password, 390144 an invalid key-pair JWT, 390303 and 390318 a bad OAuth
// token, 390422 a network policy refusing the client, 390197 MFA) or 394xxx
// (394508, 394509 MFA). Left out of those: a session that later expired or went
// away, which is not a login at all, and a role or default database the user
// cannot use, which the configuration names and a new credential does not fix.
func IsAuthError(err error) bool {
	var sfErr *gosnowflake.SnowflakeError
	if !errors.As(err, &sfErr) {
		return false
	}
	switch sfErr.Number {
	case gosnowflake.ErrSessionGone,
		390112, // session expired
		gosnowflake.ErrRoleNotExist,
		gosnowflake.ErrObjectNotExistOrAuthorized:
		return false
	case gosnowflake.ErrCodeEmptyUsernameCode,
		gosnowflake.ErrCodeEmptyPasswordCode,
		gosnowflake.ErrCodePrivateKeyParseError,
		gosnowflake.ErrCodeEmptyPasswordAndToken,
		gosnowflake.ErrCodeEmptyOAuthParameters,
		gosnowflake.ErrMissingAccessATokenButRefreshTokenPresent:
		return true
	}
	return (sfErr.Number >= 390000 && sfErr.Number < 391000) || (sfErr.Number >= 394000 && sfErr.Number < 395000)
}
