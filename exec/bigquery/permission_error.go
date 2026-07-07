package bigquery

import (
	"github.com/pkg/errors"
	"google.golang.org/api/googleapi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// IsPermissionError reports whether err indicates the DWH credentials lack the
// privileges required for the attempted operation.
func IsPermissionError(err error) bool {
	if err == nil {
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
