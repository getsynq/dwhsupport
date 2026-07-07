package redshift

import (
	"github.com/lib/pq"
	"github.com/pkg/errors"
)

// IsPermissionError reports whether err indicates the DWH credentials lack the
// privileges required for the attempted operation.
func IsPermissionError(err error) bool {
	pqError := &pq.Error{}
	if errors.As(err, &pqError) {
		return pqError.Code == "42501"
	}
	return false
}
