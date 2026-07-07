package trino

// IsPermissionError reports whether err indicates the DWH credentials lack the
// privileges required for the attempted operation.
//
// Trino-specific permission classification is not implemented yet; callers get
// the conservative answer (treat as a non-permission error).
func IsPermissionError(err error) bool {
	return false
}
