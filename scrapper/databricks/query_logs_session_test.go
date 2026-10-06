package databricks

import (
	"testing"

	"github.com/databricks/databricks-sdk-go/service/sql"
	"github.com/getsynq/dwhsupport/querylogs"
	"github.com/stretchr/testify/require"
)

func TestConvertDatabricksQueryInfoToQueryLog_SessionID(t *testing.T) {
	obfuscator, err := querylogs.NewQueryObfuscator(querylogs.ObfuscationNone)
	require.NoError(t, err)

	queryInfo := func(sessionId string) *sql.QueryInfo {
		return &sql.QueryInfo{
			QueryId:       "query-1",
			QueryText:     "SELECT 1",
			Status:        sql.QueryStatusFinished,
			StatementType: sql.QueryStatementTypeSelect,
			SessionId:     sessionId,
		}
	}

	t.Run("set", func(t *testing.T) {
		session := "01f0a2b3-c4d5-1e6f-8a9b-0c1d2e3f4a5b"
		log, err := convertDatabricksQueryInfoToQueryLog(queryInfo(session), obfuscator, "databricks", "https://test.cloud.databricks.com", "")
		require.NoError(t, err)
		require.NotNil(t, log.SessionID)
		require.Equal(t, session, *log.SessionID)
		require.Equal(t, session, log.Metadata.GetFields()["session_id"].GetStringValue())
	})

	t.Run("none", func(t *testing.T) {
		log, err := convertDatabricksQueryInfoToQueryLog(queryInfo(""), obfuscator, "databricks", "https://test.cloud.databricks.com", "")
		require.NoError(t, err)
		require.Nil(t, log.SessionID)
	})
}
