package bigquery

import (
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/getsynq/dwhsupport/querylogs"
	"github.com/stretchr/testify/require"
)

func queryHashes(hash bigquery.NullString) *struct {
	NormalizedLiterals bigquery.NullString `bigquery:"normalized_literals"`
} {
	return &struct {
		NormalizedLiterals bigquery.NullString `bigquery:"normalized_literals"`
	}{NormalizedLiterals: hash}
}

func TestConvertBigQueryRowNormalizedQueryHash(t *testing.T) {
	obfuscator, err := querylogs.NewQueryObfuscator(querylogs.ObfuscationNone)
	require.NoError(t, err)

	hash := "3e6d94758c6d57ec41e99819fa31657bb1cc39bf660837803e52243f6d24a719"
	cases := []struct {
		name   string
		hashes *struct {
			NormalizedLiterals bigquery.NullString `bigquery:"normalized_literals"`
		}
		want *string
	}{
		{name: "query job carries the hash", hashes: queryHashes(bigquery.NullString{StringVal: hash, Valid: true}), want: &hash},
		{name: "no query_info hashes, as on a load job", hashes: nil, want: nil},
		{name: "null hash", hashes: queryHashes(bigquery.NullString{}), want: nil},
		{name: "empty hash", hashes: queryHashes(bigquery.NullString{StringVal: "", Valid: true}), want: nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			row := &BigQueryQueryLogSchema{
				JobId:       bigquery.NullString{StringVal: "job-1", Valid: true},
				EndTime:     time.Date(2025, 11, 1, 10, 35, 0, 0, time.UTC),
				QueryHashes: c.hashes,
			}
			log, err := convertBigQueryRowToQueryLog(row, obfuscator, "bigquery")
			require.NoError(t, err)
			require.Equal(t, c.want, log.NormalizedQueryHash)
			require.Equal(t, "job-1", log.QueryID)
		})
	}
}

func TestBuildQueryLogsSqlReadsTheHashFromQueryInfo(t *testing.T) {
	s := &BigQueryScrapper{conf: &BigQueryScrapperConf{}}
	sql, err := s.buildQueryLogsSql(time.Now(), time.Now())
	require.NoError(t, err)
	require.Contains(t, sql, "query_info.query_hashes AS query_hashes")
	require.Equal(t, 1, strings.Count(sql, "query_hashes AS"))
}
