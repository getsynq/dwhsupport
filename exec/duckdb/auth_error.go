package duckdb

import "strings"

// IsAuthError reports whether err is MotherDuck rejecting the token. The
// extension reports it only as text, carrying the gRPC status UNAUTHENTICATED.
func IsAuthError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "unauthenticated") || strings.Contains(msg, "not authenticated")
}
