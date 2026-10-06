package redshift

import (
	"testing"
	"time"

	"github.com/getsynq/dwhsupport/querylogs"
	"github.com/stretchr/testify/require"
)

func TestConvertRedshiftRowSessionID(t *testing.T) {
	obfuscator, err := querylogs.NewQueryObfuscator(querylogs.ObfuscationNone)
	require.NoError(t, err)

	endTime := time.Date(2025, 11, 1, 10, 35, 0, 0, time.UTC)
	sessionId := int64(1073815778)

	t.Run("set", func(t *testing.T) {
		row := &RedshiftQueryLogSchema{QueryId: 42, EndTime: &endTime, SessionId: &sessionId}
		log, err := convertRedshiftRowToQueryLog(row, nil, obfuscator, "redshift", "host", "db")
		require.NoError(t, err)
		require.NotNil(t, log.SessionID)
		require.Equal(t, "1073815778", *log.SessionID)
	})

	t.Run("null", func(t *testing.T) {
		row := &RedshiftQueryLogSchema{QueryId: 42, EndTime: &endTime}
		log, err := convertRedshiftRowToQueryLog(row, nil, obfuscator, "redshift", "host", "db")
		require.NoError(t, err)
		require.Nil(t, log.SessionID)
	})
}
