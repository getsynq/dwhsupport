package oracle

import "strings"

// IsPermissionError reports whether err indicates the DWH credentials lack the
// privileges required for the attempted operation.
func IsPermissionError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	// ORA-00942: table or view does not exist
	// ORA-01031: insufficient privileges
	// ORA-00604: error occurred at recursive SQL level (often permission-related)
	return strings.Contains(errStr, "ORA-00942") ||
		strings.Contains(errStr, "ORA-01031") ||
		strings.Contains(errStr, "ORA-00604")
}
