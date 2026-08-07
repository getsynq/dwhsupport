package snowflake

import (
	"strings"

	"github.com/pkg/errors"
	gosnowflake "github.com/snowflakedb/gosnowflake"
)

// isDatabaseNotFoundError reports whether err is Snowflake refusing the session default
// database at login.
//
// Snowflake reuses ErrObjectNotExistOrAuthorized for every unresolvable object named in
// the connect string, so the code alone is ambiguous — a missing warehouse reports it
// too. Only a database is safe to drop and reconnect without; a connection with no
// warehouse cannot run a query. Hence the message check.
func isDatabaseNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	var sfErr *gosnowflake.SnowflakeError
	if !errors.As(err, &sfErr) || sfErr.Number != gosnowflake.ErrObjectNotExistOrAuthorized {
		return false
	}
	// Error() rather than Message so a message carrying MessageArgs is rendered first.
	return strings.Contains(strings.ToLower(sfErr.Error()), "database")
}
