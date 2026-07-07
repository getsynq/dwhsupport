package clickhouse

import (
	"strings"

	chgo "github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
)

// IsPermissionError reports whether err indicates the DWH credentials lack the
// privileges required for the attempted operation.
func IsPermissionError(err error) bool {
	if err == nil {
		return false
	}
	// Code 497: NOT_ENOUGH_PRIVILEGES (not defined as constant in ch-go)
	if chgo.IsErr(err, proto.Error(497)) {
		return true
	}
	return strings.Contains(err.Error(), "Not enough privileges")
}
