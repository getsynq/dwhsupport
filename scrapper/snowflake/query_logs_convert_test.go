package snowflake

import (
	"math/rand"
	"strconv"
	"strings"
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

// QUERY_HISTORY keeps a statement of more than 100 000 characters whole, so a dbt build of that
// size is not truncated and has to be parsed.
func TestConvertSnowflakeRowLongTextIsNotTruncated(t *testing.T) {
	obfuscator, err := querylogs.NewQueryObfuscator(querylogs.ObfuscationNone)
	require.NoError(t, err)

	text := "create or replace transient table DB.SCHEMA.MODEL__dbt_tmp as (select 1 as id /* " +
		strings.Repeat("x", 150_000) + " */)"
	row := &SnowflakeQueryLogSchema{QueryID: "q1", ExecutionStatus: "SUCCESS", QueryText: text}
	log, err := convertSnowflakeRowToQueryLog(row, obfuscator, "snowflake", "account")
	require.NoError(t, err)
	require.False(t, log.IsTruncated)
	require.Equal(t, text, log.SQL)
}

func TestSnowflakeQueryTextMayBeTruncated(t *testing.T) {
	rnd := rand.New(rand.NewSource(1))
	random := func(n int) string {
		const alnum = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 "
		b := make([]byte, n)
		for i := range b {
			b[i] = alnum[rnd.Intn(len(alnum))]
		}
		return string(b)
	}

	// Under 100 000 bytes nothing is cut, however badly it compresses.
	require.False(t, snowflakeQueryTextMayBeTruncated(random(99_000)))
	// A cut never goes below 100 000 bytes, which is where text that does not compress ends up.
	require.True(t, snowflakeQueryTextMayBeTruncated(random(100_000)))
	// A megabyte whose snappy encoding is under 50 000 bytes is whole.
	require.False(t, snowflakeQueryTextMayBeTruncated(strings.Repeat("select 1 as id union all\n", 40_000)))
	// A prefix whose snappy size is in the band a cut lands in may be cut.
	require.True(t, snowflakeQueryTextMayBeTruncated(random(95_000)+strings.Repeat("x", 100_000)))
}
