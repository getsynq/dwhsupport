package duckdb

import (
	duckdb "github.com/duckdb/duckdb-go/v2"
	"github.com/pkg/errors"
)

// IsPermissionError reports whether err indicates the DWH credentials lack the
// privileges required for the attempted operation.
func IsPermissionError(err error) bool {
	duckdbError := &duckdb.Error{}
	if errors.As(err, &duckdbError) {
		return duckdbError.Type == duckdb.ErrorTypePermission
	}
	return false
}
