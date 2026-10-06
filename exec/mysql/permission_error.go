package mysql

import (
	"github.com/go-sql-driver/mysql"
	"github.com/pkg/errors"
)

// Server error numbers that mean the connecting account lacks a privilege.
// They are the same on MySQL and MariaDB.
const (
	// ER_DBACCESS_DENIED_ERROR: no access to a database.
	errDatabaseAccessDenied = 1044
	// ER_TABLEACCESS_DENIED_ERROR: "SELECT command denied to user ... for
	// table 'user'", what reading mysql.user without SELECT on it gives.
	errTableAccessDenied = 1142
	// ER_COLUMNACCESS_DENIED_ERROR: a column the account may not read.
	errColumnAccessDenied = 1143
	// ER_SPECIFIC_ACCESS_DENIED_ERROR: a statement that needs a global
	// privilege such as PROCESS or CREATE USER.
	errSpecificAccessDenied = 1227
)

// IsPermissionError reports whether err indicates the DWH credentials lack the
// privileges required for the attempted operation.
func IsPermissionError(err error) bool {
	mySqlError := &mysql.MySQLError{}
	if errors.As(err, &mySqlError) {
		switch mySqlError.Number {
		case errDatabaseAccessDenied, errTableAccessDenied, errColumnAccessDenied, errSpecificAccessDenied:
			return true
		}
	}
	return false
}
