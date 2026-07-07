package databricks

import "strings"

// IsPermissionError reports whether err indicates the DWH credentials lack the
// privileges required for the attempted operation.
func IsPermissionError(err error) bool {
	if err == nil {
		return false
	}
	errMsg := err.Error()
	return strings.Contains(errMsg, "PERMISSION_DENIED") ||
		strings.Contains(errMsg, "ACCESS_DENIED") ||
		strings.Contains(errMsg, "does not have permission")
}
