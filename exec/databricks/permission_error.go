package databricks

import (
	"strings"

	"github.com/databricks/databricks-sdk-go/apierr"
	"github.com/pkg/errors"
)

// IsPermissionError reports whether err indicates the DWH credentials lack the
// privileges required for the attempted operation.
//
// A REST refusal is recognised by its 403 status, which the SDK maps to
// apierr.ErrPermissionDenied: the SDK's error text is only the message, and SCIM
// words a refusal as "Only workspace admins can ..." with no code in it.
func IsPermissionError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, apierr.ErrPermissionDenied) {
		return true
	}
	errMsg := err.Error()
	return strings.Contains(errMsg, "PERMISSION_DENIED") ||
		strings.Contains(errMsg, "ACCESS_DENIED") ||
		strings.Contains(errMsg, "does not have permission")
}
