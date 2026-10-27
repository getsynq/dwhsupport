package bigquery

import (
	"github.com/pkg/errors"
	"google.golang.org/api/googleapi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// IsPermissionError reports whether err indicates the DWH credentials lack the
// privileges required for the attempted operation. A refused impersonation hop is not one, even
// though Google answers it with a 403: no identity was established, which IsAuthError reports.
func IsPermissionError(err error) bool {
	if err == nil {
		return false
	}
	if _, ok := AsImpersonationError(err); ok {
		return false
	}
	if code := status.Code(err); code == codes.PermissionDenied {
		return true
	}
	var apiErr *googleapi.Error
	if errors.As(err, &apiErr) {
		if apiErr.Code == 403 {
			return true
		}
	}
	return false
}
