package athena

import (
	"github.com/aws/smithy-go"
	"github.com/pkg/errors"
)

// authErrorCodes are the codes AWS answers a request with when it does not
// accept the credentials that signed it. STS spells a refused AssumeRole
// AccessDenied, the role's trust policy not letting this identity in. Athena's
// own AccessDeniedException is not here: the identity was accepted and lacks a
// grant, which IsPermissionError reports.
var authErrorCodes = map[string]bool{
	"InvalidClientTokenId":        true,
	"SignatureDoesNotMatch":       true,
	"IncompleteSignature":         true,
	"ExpiredToken":                true,
	"ExpiredTokenException":       true,
	"UnrecognizedClientException": true,
	"InvalidSignatureException":   true,
	"AccessDenied":                true,
}

// IsAuthError reports whether err is AWS rejecting the credentials.
func IsAuthError(err error) bool {
	var apiErr smithy.APIError
	return errors.As(err, &apiErr) && authErrorCodes[apiErr.ErrorCode()]
}
