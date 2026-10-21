package duckdb

import (
	"testing"

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
			"token rejected",
			errors.New(
				"Invalid Input Error: Your request is not authenticated. Please check your MotherDuck token. (UNAUTHENTICATED, request id: x)",
			),
			true,
		},
		{"unreachable", errors.New("IO Error: Failed to connect to MotherDuck: UNAVAILABLE: failed to connect to all addresses"), false},
		{"nil", nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, IsAuthError(tc.err))
		})
	}
}
