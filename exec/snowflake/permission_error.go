package snowflake

import (
	"strings"

	"github.com/pkg/errors"
	gosnowflake "github.com/snowflakedb/gosnowflake"
)

// IsPermissionError reports whether err is a Snowflake failure that is
// user-actionable and NOT worth retrying — callers use it to finish the job and
// surface the cause to the customer instead of looping with backoff.
// Recognises:
//   - SQL compilation error 002003 with the canonical "does not exist or not authorized"
//     message (most common shape — Snowflake intentionally conflates "no privilege" and
//     "no such object" to prevent object-existence probing).
//   - Error 003001 "Insufficient privileges to operate on ...".
//   - Error 090073 "Warehouse '%s' cannot be resumed because resource monitor '%s'
//     has exceeded its quota." — the customer's compute is capped/suspended; retrying
//     just builds a multi-day job backlog (QUA-602). Only the customer can lift it.
//
// This is the single source of truth for Snowflake permission classification,
// shared by the scrapper and the audit/query paths.
func IsPermissionError(err error) bool {
	if err == nil {
		return false
	}
	var sfErr *gosnowflake.SnowflakeError
	if errors.As(err, &sfErr) {
		if sfErr.Number == 3001 || sfErr.Number == 90073 {
			return true
		}
	}
	return strings.Contains(err.Error(), "does not exist or not authorized") ||
		strings.Contains(err.Error(), "cannot be resumed because resource monitor")
}
