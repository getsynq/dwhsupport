package redshift

import (
	"github.com/lib/pq"
	"github.com/pkg/errors"
)

// IsAuthError reports whether err is the cluster rejecting the login: a wrong
// password (28P01) or a user it does not let in (28000).
func IsAuthError(err error) bool {
	pqError := &pq.Error{}
	if errors.As(err, &pqError) {
		return pqError.Code == "28P01" || pqError.Code == "28000"
	}
	return false
}
