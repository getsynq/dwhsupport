package mssql

import "strings"

// IsPermissionError reports whether err indicates the DWH credentials lack the
// privileges required for the attempted operation.
func IsPermissionError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	// Msg 229: The SELECT permission was denied
	// Msg 230: The SELECT permission was denied on column
	// Msg 262: CREATE DATABASE permission denied
	return strings.Contains(errStr, "permission was denied") ||
		strings.Contains(errStr, "permission denied")
}
