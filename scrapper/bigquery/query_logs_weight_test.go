package bigquery

import (
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/getsynq/dwhsupport/querylogs"
	"github.com/stretchr/testify/require"
)

func TestBigQueryQueryLogWeight(t *testing.T) {
	obfuscator, err := querylogs.NewQueryObfuscator(querylogs.ObfuscationNone)
	require.NoError(t, err)
	ptr := func(v int64) *int64 { return &v }

	start := time.Date(2026, 10, 8, 14, 58, 2, 197_000_000, time.UTC)
	row := &BigQueryQueryLogSchema{
		CreationTime:        start,
		StartTime:           start,
		EndTime:             start.Add(690 * time.Millisecond),
		ProjectId:           bigquery.NullString{StringVal: "my-project", Valid: true},
		JobId:               bigquery.NullString{StringVal: "job-1", Valid: true},
		JobType:             bigquery.NullString{StringVal: "QUERY", Valid: true},
		StatementType:       bigquery.NullString{StringVal: "SELECT", Valid: true},
		State:               bigquery.NullString{StringVal: "DONE", Valid: true},
		ReservationId:       bigquery.NullString{StringVal: "admin-project:EU.default", Valid: true},
		TotalSlotMs:         bigquery.NullInt64{Int64: 688, Valid: true},
		TotalBytesProcessed: bigquery.NullInt64{Int64: 109_557, Valid: true},
		TotalBytesBilled:    bigquery.NullInt64{Int64: 10_485_760, Valid: true},
	}
	log, err := convertBigQueryRowToQueryLog(row, obfuscator, "bigquery")
	require.NoError(t, err)
	require.Equal(t, querylogs.QueryWeight{
		ExecutionMs: ptr(690), ComputeMs: ptr(688), BytesScanned: ptr(109_557), BytesBilled: ptr(10_485_760),
		ComputeId: "admin-project:EU.default",
	}, log.Weight())

	row.ReservationId = bigquery.NullString{}
	row.TotalSlotMs = bigquery.NullInt64{}
	row.TotalBytesProcessed = bigquery.NullInt64{}
	row.TotalBytesBilled = bigquery.NullInt64{}
	log, err = convertBigQueryRowToQueryLog(row, obfuscator, "bigquery")
	require.NoError(t, err)
	require.Equal(t, querylogs.QueryWeight{ExecutionMs: ptr(690), ComputeId: "my-project"}, log.Weight(),
		"on demand, the project is the compute, and figures the job did not report stay empty")
}

// BI Engine bills nothing for the bytes it reads from memory, so its mode is what tells a query it
// answered from one that billed nothing for another reason.
func TestBigQueryQueryLogRecordsTheBiEngineMode(t *testing.T) {
	obfuscator, err := querylogs.NewQueryObfuscator(querylogs.ObfuscationNone)
	require.NoError(t, err)

	row := &BigQueryQueryLogSchema{
		JobId:        bigquery.NullString{StringVal: "job-1", Valid: true},
		EndTime:      time.Date(2026, 10, 12, 9, 6, 26, 0, time.UTC),
		BiEngineMode: bigquery.NullString{StringVal: "FULL", Valid: true},
	}
	log, err := convertBigQueryRowToQueryLog(row, obfuscator, "bigquery")
	require.NoError(t, err)
	require.Equal(t, "FULL", log.Metadata.GetFields()["bi_engine_mode"].GetStringValue())

	row.BiEngineMode = bigquery.NullString{}
	log, err = convertBigQueryRowToQueryLog(row, obfuscator, "bigquery")
	require.NoError(t, err)
	require.NotContains(t, log.Metadata.GetFields(), "bi_engine_mode", "a job BI Engine did not see has no mode")
}

func TestBuildQueryLogsSqlReadsTheBiEngineMode(t *testing.T) {
	s := &BigQueryScrapper{conf: &BigQueryScrapperConf{}}
	sql, err := s.buildQueryLogsSql(time.Now(), time.Now())
	require.NoError(t, err)
	require.Contains(t, sql, "bi_engine_statistics.bi_engine_mode AS bi_engine_mode")
	require.Contains(t, sql, "total_bytes_billed")
}
