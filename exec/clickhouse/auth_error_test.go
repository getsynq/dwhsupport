package clickhouse

import (
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
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
			"authentication failed",
			&clickhouse.Exception{Code: 516, Name: "AUTHENTICATION_FAILED", Message: "u: Authentication failed: password is incorrect"},
			true,
		},
		{"unknown user", errors.Wrap(&clickhouse.Exception{Code: 192, Name: "UNKNOWN_USER"}, "ping"), true},
		{"read EOF", errors.New("read: EOF"), false},
		{"no such host", errors.New("dial tcp: lookup clickhouse.example.com: no such host"), false},
		{"missing grant", &clickhouse.Exception{Code: 497, Name: "ACCESS_DENIED"}, false},
		{"nil", nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, IsAuthError(tc.err))
		})
	}
}
