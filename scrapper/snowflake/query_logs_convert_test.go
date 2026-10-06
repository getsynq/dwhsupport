package snowflake

import (
	"strconv"
	"testing"
	"time"

	"github.com/getsynq/dwhsupport/querylogs"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"
)

// Snowflake session and transaction ids run past 2^53, where a float64 can no longer hold every
// integer, so a number in the metadata struct comes back with its low bits gone.
func TestConvertSnowflakeRowKeepsLargeIdsExact(t *testing.T) {
	obfuscator, err := querylogs.NewQueryObfuscator(querylogs.ObfuscationNone)
	require.NoError(t, err)

	row := &SnowflakeQueryLogSchema{
		QueryID:         "01b2c3d4-0000-0000-0000-000000000001",
		ExecutionStatus: "SUCCESS",
		SessionID:       32791298003718697,
		TransactionID:   1759751234567890123,
		EndTime:         time.Date(2025, 11, 1, 10, 35, 0, 0, time.UTC),
	}

	log, err := convertSnowflakeRowToQueryLog(row, obfuscator, "snowflake", "account")
	require.NoError(t, err)
	require.Equal(t, "32791298003718697", metadataId(log.Metadata.GetFields()["session_id"]))
	require.Equal(t, "1759751234567890123", metadataId(log.Metadata.GetFields()["transaction_id"]))
}

// metadataId reads an id the way a consumer does, whether it was written as a number or a string.
func metadataId(v *structpb.Value) string {
	if _, ok := v.GetKind().(*structpb.Value_NumberValue); ok {
		return strconv.FormatInt(int64(v.GetNumberValue()), 10)
	}
	return v.GetStringValue()
}

func TestConvertSnowflakeRowSessionID(t *testing.T) {
	obfuscator, err := querylogs.NewQueryObfuscator(querylogs.ObfuscationNone)
	require.NoError(t, err)

	t.Run("above 2^53, exact", func(t *testing.T) {
		row := &SnowflakeQueryLogSchema{QueryID: "q1", ExecutionStatus: "SUCCESS", SessionID: 32791298003718697}
		log, err := convertSnowflakeRowToQueryLog(row, obfuscator, "snowflake", "account")
		require.NoError(t, err)
		require.NotNil(t, log.SessionID)
		require.Equal(t, "32791298003718697", *log.SessionID)
	})

	t.Run("a view configured as AccountUsageDb without SESSION_ID", func(t *testing.T) {
		row := &SnowflakeQueryLogSchema{QueryID: "q1", ExecutionStatus: "SUCCESS"}
		log, err := convertSnowflakeRowToQueryLog(row, obfuscator, "snowflake", "account")
		require.NoError(t, err)
		require.Nil(t, log.SessionID)
	})
}
