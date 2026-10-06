package bigquery

import (
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/getsynq/dwhsupport/querylogs"
	"github.com/stretchr/testify/require"
)

func TestConvertBigQueryRowSessionID(t *testing.T) {
	obfuscator, err := querylogs.NewQueryObfuscator(querylogs.ObfuscationNone)
	require.NoError(t, err)

	session := bigquery.NullString{StringVal: "CgwKBnByb2plY3QQARoAEiQ0YjJmY2E3Ni0wMDAwLTI1ZjMtYTRjMi0wODllMDgyMDg4ZTA=", Valid: true}
	parent := bigquery.NullString{StringVal: "script_job_0123456789abcdef", Valid: true}
	cases := []struct {
		name    string
		session bigquery.NullString
		parent  bigquery.NullString
		want    *string
	}{
		{name: "the session wins over the script", session: session, parent: parent, want: &session.StringVal},
		{name: "a job run in a session", session: session, want: &session.StringVal},
		{name: "a statement of a script outside a session", parent: parent, want: &parent.StringVal},
		{name: "neither", want: nil},
		{name: "empty values", session: bigquery.NullString{Valid: true}, parent: bigquery.NullString{Valid: true}, want: nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			row := &BigQueryQueryLogSchema{
				JobId:       bigquery.NullString{StringVal: "job-1", Valid: true},
				EndTime:     time.Date(2025, 11, 1, 10, 35, 0, 0, time.UTC),
				SessionId:   c.session,
				ParentJobId: c.parent,
			}
			log, err := convertBigQueryRowToQueryLog(row, obfuscator, "bigquery")
			require.NoError(t, err)
			require.Equal(t, c.want, log.SessionID)
		})
	}
}

func TestBuildQueryLogsSqlReadsTheSessionFromSessionInfo(t *testing.T) {
	s := &BigQueryScrapper{conf: &BigQueryScrapperConf{}}
	sql, err := s.buildQueryLogsSql(time.Now(), time.Now())
	require.NoError(t, err)
	require.Contains(t, sql, "session_info.session_id AS session_id")
	require.Equal(t, 1, strings.Count(sql, "session_id AS"))
	require.Contains(t, sql, "parent_job_id")
}
