package clickhouse

import (
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/pkg/errors"
)

// IsAuthError reports whether err is the server rejecting the login.
func IsAuthError(err error) bool {
	var exception *clickhouse.Exception
	if !errors.As(err, &exception) {
		return false
	}
	switch exception.Code {
	case 516, // AUTHENTICATION_FAILED
		192, // UNKNOWN_USER
		193, // WRONG_PASSWORD
		194: // REQUIRED_PASSWORD
		return true
	}
	return false
}
