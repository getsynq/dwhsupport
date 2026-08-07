package snowflake

import (
	"testing"

	"github.com/pkg/errors"
	gosnowflake "github.com/snowflakedb/gosnowflake"
	"github.com/stretchr/testify/assert"
)

// Snowflake error numbers the driver does not name.
const (
	errIncorrectUsernameOrPassword = 390100
	errJWTTokenInvalid             = 390144
	errSQLCompilationFail          = 2003
)

func TestIsDatabaseNotFoundError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{
			name: "database does not exist",
			err: &gosnowflake.SnowflakeError{
				Number:   gosnowflake.ErrObjectNotExistOrAuthorized,
				SQLState: "08004",
				Message:  "The requested database does not exist or not authorized.",
			},
			want: true,
		},
		{
			name: "wrapped database does not exist",
			err: errors.Wrap(&gosnowflake.SnowflakeError{
				Number:   gosnowflake.ErrObjectNotExistOrAuthorized,
				SQLState: "08004",
				Message:  "The requested database does not exist or not authorized.",
			}, "connection error"),
			want: true,
		},
		{
			// Message casing is not guaranteed, so the match is case-insensitive.
			name: "database in a different case",
			err: &gosnowflake.SnowflakeError{
				Number:   gosnowflake.ErrObjectNotExistOrAuthorized,
				SQLState: "08004",
				Message:  "The requested DATABASE does not exist or not authorized.",
			},
			want: true,
		},
		{
			// The message is matched after rendering, so a template carrying MessageArgs
			// still classifies correctly.
			err: &gosnowflake.SnowflakeError{
				Number:      gosnowflake.ErrObjectNotExistOrAuthorized,
				SQLState:    "08004",
				Message:     "The requested %s does not exist or not authorized.",
				MessageArgs: []any{"database"},
			},
			name: "message rendered from args",
			want: true,
		},
		{
			name: "warehouse shares the error code",
			err: &gosnowflake.SnowflakeError{
				Number:   gosnowflake.ErrObjectNotExistOrAuthorized,
				SQLState: "08004",
				Message:  "The requested warehouse does not exist or not authorized.",
			},
			want: false,
		},
		{
			// Keeps the error-number check load-bearing: a query-time compilation error
			// mentions a database but is not the login-time failure the retry addresses.
			name: "message mentions a database but the code is not a connect failure",
			err: &gosnowflake.SnowflakeError{
				Number:   errSQLCompilationFail,
				SQLState: "02000",
				Message:  "SQL compilation error:\nDatabase 'FOO' does not exist or not authorized.",
			},
			want: false,
		},
		{
			// A rejected key pair must NOT be mistaken for a config problem: the
			// connection retry only makes sense when credentials are known good.
			name: "invalid jwt is not a database problem",
			err: &gosnowflake.SnowflakeError{
				Number:   errJWTTokenInvalid,
				SQLState: "08004",
				Message:  "JWT token is invalid.",
			},
			want: false,
		},
		{
			name: "bad role is a different code entirely",
			err: &gosnowflake.SnowflakeError{
				Number:   gosnowflake.ErrRoleNotExist,
				SQLState: "08004",
				Message:  "Role 'NO_SUCH_ROLE' specified in the connect string does not exist or not authorized.",
			},
			want: false,
		},
		{
			name: "non-snowflake error",
			err:  errors.New("dial tcp: connection refused"),
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, isDatabaseNotFoundError(tt.err))
		})
	}
}
