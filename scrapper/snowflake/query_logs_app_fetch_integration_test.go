package snowflake

import (
	"context"
	"testing"

	"github.com/getsynq/dwhsupport/sqldialect"
	"github.com/stretchr/testify/require"
)

// Snowflake's regular expressions are not Go's, so the pattern that admits an app's own file fetch
// past a database filter is checked by the warehouse that runs it, rendered the way the query-log
// fetch renders it.
func TestSnowflakeAppFileFetchPatternMatchesInSnowflake(t *testing.T) {
	skipWithoutSnowflake(t)
	sc, err := NewSnowflakeScrapper(context.Background(), &SnowflakeScrapperConf{SnowflakeConf: snowflakeTestConf()})
	if err != nil {
		t.Skipf("Could not connect to Snowflake: %v", err)
	}
	defer sc.Close()

	cases := map[string]bool{
		`GET 'snow://streamlit/"APPS_DB"."PUBLIC"."MY_APP"/versions/version$1/snowflake.yml' 'file:///opt/streamlit-runtime'`: true,
		`ls 'snow://streamlit/APPS_DB.PUBLIC.MY_APP/versions/live/'`:                                                          true,
		"\n  GET 'snow://notebook/\"APPS_DB\".\"PUBLIC\".\"MY_NOTEBOOK\"/versions/live/notebook_app.ipynb'\n 'file:///tmp'":   true,
		`GET 'snow://workspace/USER$.PUBLIC.DEFAULT$/versions/head/query.sql' 'file:///tmp'`:                                  false,
		`GET @my_stage/file.csv file:///tmp`:                       false,
		`SELECT 'GET ''snow://streamlit/APPS_DB.PUBLIC.MY_APP/'''`: false,
	}
	dialect := sqldialect.NewSnowflakeDialect()
	for text, want := range cases {
		cond, err := sqldialect.FnCond("REGEXP_LIKE",
			sqldialect.String(text), sqldialect.String(appFileFetchPattern), sqldialect.String("is"),
		).ToSql(dialect)
		require.NoError(t, err)
		var got []bool
		require.NoError(t, sc.Executor().Select(context.Background(), &got, "SELECT "+cond))
		require.Equal(t, []bool{want}, got, text)
	}
}
