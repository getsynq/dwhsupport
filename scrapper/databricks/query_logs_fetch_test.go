package databricks

import (
	"context"
	"io"
	"testing"
	"time"

	serviceiam "github.com/databricks/databricks-sdk-go/service/iam"
	servicesql "github.com/databricks/databricks-sdk-go/service/sql"
	"github.com/getsynq/dwhsupport/querylogs"
	"github.com/stretchr/testify/require"
)

// TestFetchQueryLogsThroughTheWorkspaceApi drives FetchQueryLogs against a workspace that
// answers the Query History and SCIM endpoints, so what it checks is what a fetch hands its
// caller: the redacted statements kept with empty SQL and the marker, and service principals
// named without losing the application id they logged in as.
func TestFetchQueryLogsThroughTheWorkspaceApi(t *testing.T) {
	const (
		etlApplicationId     = "8b2d6a3e-1f4c-4b7a-9c0e-2d5f6a7b8c9d"
		unnamedApplicationId = "1c9e2f40-6a1b-4d3e-8f2a-7b6c5d4e3f21"
	)
	finished := time.Now().Add(-time.Minute)
	query := func(id, text, userName string) servicesql.QueryInfo {
		return servicesql.QueryInfo{
			QueryId:          id,
			QueryText:        text,
			QueryStartTimeMs: finished.Add(-time.Second).UnixMilli(),
			QueryEndTimeMs:   finished.UnixMilli(),
			Status:           servicesql.QueryStatusFinished,
			StatementType:    servicesql.QueryStatementTypeSelect,
			UserName:         userName,
		}
	}
	history := []servicesql.QueryInfo{
		query("readable", "SELECT * FROM main.sales.orders", "analyst@example.com"),
		query("redacted-human", "<REDACTED>", "analyst@example.com"),
		query("redacted-etl", "<REDACTED>", etlApplicationId),
		query("redacted-etl-again", "<REDACTED>", etlApplicationId),
		query("unnamed-principal", "SELECT 1", unnamedApplicationId),
	}

	fetch := func(t *testing.T, workspace *fakeWorkspace) map[string]*querylogs.QueryLog {
		t.Helper()
		scrapper := workspace.start(t)
		obfuscator, err := querylogs.NewQueryObfuscator(querylogs.ObfuscationNone)
		require.NoError(t, err)

		iter, err := scrapper.FetchQueryLogs(context.Background(), finished.Add(-time.Hour), time.Now(), obfuscator)
		require.NoError(t, err)
		defer iter.Close()

		logs := map[string]*querylogs.QueryLog{}
		for {
			log, err := iter.Next(context.Background())
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
			logs[log.QueryID] = log
		}
		return logs
	}

	t.Run("redacted_and_named", func(t *testing.T) {
		workspace := newFakeWorkspace()
		workspace.queryHistory = history
		workspace.servicePrincipals = []serviceiam.ServicePrincipal{{ApplicationId: etlApplicationId, DisplayName: "etl-runner"}}

		logs := fetch(t, workspace)
		require.Len(t, logs, len(history))

		require.Equal(t, "SELECT * FROM main.sales.orders", logs["readable"].SQL)
		require.False(t, logs["readable"].IsTextRedacted())

		for _, id := range []string{"redacted-human", "redacted-etl", "redacted-etl-again"} {
			require.Empty(t, logs[id].SQL, id)
			require.True(t, logs[id].IsTextRedacted(), id)
		}

		etl := logs["redacted-etl"]
		require.Equal(t, etlApplicationId, etl.DwhContext.User)
		require.Equal(t, "etl-runner", etl.Metadata.GetFields()["user_display_name"].GetStringValue())
		require.NotContains(t, logs["redacted-human"].Metadata.GetFields(), "user_display_name")
		require.NotContains(t, logs["unnamed-principal"].Metadata.GetFields(), "user_display_name")

		// One lookup per distinct application id, none for a human login.
		require.ElementsMatch(t, []string{
			`applicationId eq "` + etlApplicationId + `"`,
			`applicationId eq "` + unnamedApplicationId + `"`,
		}, workspace.lookedUpPrincipals)
	})

	t.Run("service_principal_listing_refused", func(t *testing.T) {
		workspace := newFakeWorkspace()
		workspace.queryHistory = history
		workspace.denyServicePrincipals = true

		logs := fetch(t, workspace)
		require.Len(t, logs, len(history), "a refused lookup must not cost the fetch any query")
		require.Equal(t, etlApplicationId, logs["redacted-etl"].DwhContext.User)
		require.NotContains(t, logs["redacted-etl"].Metadata.GetFields(), "user_display_name")
		require.Len(t, workspace.lookedUpPrincipals, 1, "resolution stops after the first refusal")
	})
}
