package trino

import (
	"net/http"

	"github.com/pkg/errors"
	"github.com/trinodb/trino-go-client/trino"
)

// IsAuthError reports whether err is the coordinator rejecting the credentials.
func IsAuthError(err error) bool {
	var queryFailed *trino.ErrQueryFailed
	return errors.As(err, &queryFailed) && queryFailed.StatusCode == http.StatusUnauthorized
}
