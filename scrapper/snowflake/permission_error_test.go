package snowflake

import (
	"testing"

	"github.com/pkg/errors"
	gosnowflake "github.com/snowflakedb/gosnowflake"
	"github.com/stretchr/testify/assert"
)

func TestIsPermissionError(t *testing.T) {
	t.Run("nil", func(t *testing.T) {
		assert.False(t, IsPermissionError(nil))
	})

	t.Run("does not exist or not authorized — bare", func(t *testing.T) {
		err := errors.New("002003 (02000): SQL compilation error:\nSchema 'SNOWFLAKE.ACCOUNT_USAGE' does not exist or not authorized.")
		assert.True(t, IsPermissionError(err))
	})

	t.Run("does not exist or not authorized — wrapped", func(t *testing.T) {
		inner := errors.New("Object 'FOO' does not exist or not authorized.")
		err := errors.Wrap(inner, "could not get columns for table SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY")
		assert.True(t, IsPermissionError(err))
	})

	t.Run("SnowflakeError 3001 insufficient privileges", func(t *testing.T) {
		err := &gosnowflake.SnowflakeError{Number: 3001, Message: "Insufficient privileges to operate on schema 'X'"}
		assert.True(t, IsPermissionError(err))
	})

	t.Run("warehouse cannot be resumed — resource monitor quota (typed 90073)", func(t *testing.T) {
		err := &gosnowflake.SnowflakeError{
			Number:  90073,
			Message: "Warehouse 'WH_DATA_GOVERNANCE' cannot be resumed because resource monitor 'GOVERNANCE_RM' has exceeded its quota.",
		}
		assert.True(t, IsPermissionError(err))
	})

	t.Run("warehouse cannot be resumed — resource monitor quota (string, wrapped)", func(t *testing.T) {
		inner := errors.New("090073 (22000): Warehouse 'WH_DATA_GOVERNANCE' cannot be resumed because resource monitor 'GOVERNANCE_RM' has exceeded its quota.")
		err := errors.Wrap(inner, "failed to fetch query logs")
		assert.True(t, IsPermissionError(err))
	})

	t.Run("unrelated SnowflakeError", func(t *testing.T) {
		err := &gosnowflake.SnowflakeError{Number: 1234, Message: "syntax error"}
		assert.False(t, IsPermissionError(err))
	})

	t.Run("generic error", func(t *testing.T) {
		assert.False(t, IsPermissionError(errors.New("connection reset by peer")))
	})
}
