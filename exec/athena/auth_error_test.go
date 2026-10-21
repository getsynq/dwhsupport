package athena

import (
	"fmt"
	"net"
	"testing"

	"github.com/aws/smithy-go"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestIsAuthError(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{
			"unknown access key",
			fmt.Errorf(
				"athena: sts:GetCallerIdentity: %w",
				&smithy.GenericAPIError{Code: "InvalidClientTokenId", Message: "The security token included in the request is invalid."},
			),
			true,
		},
		{"wrong secret", &smithy.GenericAPIError{Code: "SignatureDoesNotMatch"}, true},
		{"role trust refuses AssumeRole", errors.Wrap(&smithy.GenericAPIError{Code: "AccessDenied"}, "resolve credentials"), true},
		{"refused connection", &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connect: connection refused")}, false},
		{"missing Athena grant", &smithy.GenericAPIError{Code: "AccessDeniedException"}, false},
		{"throttled", &smithy.GenericAPIError{Code: "ThrottlingException"}, false},
		{"nil", nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, IsAuthError(tc.err))
		})
	}
}
