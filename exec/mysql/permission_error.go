package mysql

import (
	"github.com/go-sql-driver/mysql"
	"github.com/pkg/errors"
)

// IsPermissionError reports whether err indicates the DWH credentials lack the
// privileges required for the attempted operation.
func IsPermissionError(err error) bool {
	mySqlError := &mysql.MySQLError{}
	if errors.As(err, &mySqlError) {
		return mySqlError.Number == 1044
	}
	return false
}
