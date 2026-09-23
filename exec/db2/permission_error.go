package db2

import "regexp"

// permissionErrors matches the SQLCODEs and SQLSTATEs Db2 answers a missing
// privilege with. The driver writes an SQLCA as `SQLCODE=-551 SQLSTATE=42501`
// and a server diagnostic as `SQLCODE:-551`, so both separators are accepted.
//
//	-551 / 42501  the authorization ID lacks a privilege on an object
//	-552 / 42502  the authorization ID lacks the privilege to run the statement
//	-1060 / 08004 the authorization ID lacks CONNECT on the database
var permissionErrors = regexp.MustCompile(`SQLCODE[=:]\s*-(551|552|1060)\b|SQLSTATE[=:]\s*(42501|42502)\b`)

// IsPermissionError reports whether err indicates the DWH credentials lack the
// privileges required for the attempted operation.
func IsPermissionError(err error) bool {
	if err == nil {
		return false
	}
	return permissionErrors.MatchString(err.Error())
}
