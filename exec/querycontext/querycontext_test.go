package querycontext

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWithQueryContext_RoundTrip(t *testing.T) {
	ctx := context.Background()
	assert.Nil(t, GetQueryContext(ctx))

	qc := QueryContext{"source": "synq", "monitor": "foo"}
	ctx = WithQueryContext(ctx, qc)

	got := GetQueryContext(ctx)
	require.NotNil(t, got)
	assert.Equal(t, "synq", got["source"])
	assert.Equal(t, "foo", got["monitor"])
}

func TestFormatAsJSON(t *testing.T) {
	assert.Equal(t, "", QueryContext(nil).FormatAsJSON())
	assert.Equal(t, "", QueryContext{}.FormatAsJSON())

	qc := QueryContext{"source": "synq"}
	assert.Equal(t, `{"source":"synq"}`, qc.FormatAsJSON())
}

func TestFormatAsSQLComment(t *testing.T) {
	assert.Equal(t, "", QueryContext(nil).FormatAsSQLComment())
	assert.Equal(t, "", QueryContext{}.FormatAsSQLComment())

	qc := QueryContext{"source": "synq"}
	assert.Equal(t, ` /* {"source":"synq"} */`, qc.FormatAsSQLComment())
}

func TestAppendSQLComment(t *testing.T) {
	ctx := context.Background()
	sql := "SELECT 1"

	// No query context — unchanged
	assert.Equal(t, "SELECT 1", AppendSQLComment(ctx, sql))

	// With query context — appended
	ctx = WithQueryContext(ctx, QueryContext{"source": "synq"})
	result := AppendSQLComment(ctx, sql)
	assert.Equal(t, `SELECT 1 /* {"source":"synq"} */`, result)
}

func TestAppendSQLComment_StripsTrailingSemicolon(t *testing.T) {
	ctx := WithQueryContext(context.Background(), QueryContext{"source": "synq"})

	result := AppendSQLComment(ctx, "SELECT 1;")
	assert.Equal(t, `SELECT 1 /* {"source":"synq"} */`, result)

	result = AppendSQLComment(ctx, "SELECT 1 ;  \n")
	assert.Equal(t, `SELECT 1 /* {"source":"synq"} */`, result)
}

func TestAppendSQLLineComment(t *testing.T) {
	assert.Equal(t, "SHOW CREATE TABLE t", AppendSQLLineComment(context.Background(), "SHOW CREATE TABLE t"), "no query context")
	assert.Equal(t, "SHOW CREATE TABLE t", AppendSQLLineComment(WithQueryContext(context.Background(), QueryContext{}), "SHOW CREATE TABLE t"))

	ctx := WithQueryContext(context.Background(), QueryContext{"source": "synq"})
	assert.Equal(t, "SHOW CREATE TABLE t\n-- {\"source\":\"synq\"}", AppendSQLLineComment(ctx, "SHOW CREATE TABLE t;  \n"))
}

// Every line break a key or a value can carry stays escaped, so nothing after
// the line comment's own line can be read as SQL.
func TestSQLLineCommentStaysOnOneLine(t *testing.T) {
	for _, value := range []string{"a\nDROP TABLE x", "a\rDROP TABLE x", "a\u2028DROP TABLE x", "a\u2029DROP TABLE x"} {
		comment := QueryContext{"note\nkey": value}.FormatAsSQLLineComment()
		require.True(t, strings.HasPrefix(comment, "\n-- {"), "%q", comment)
		body := comment[len("\n"):]
		assert.NotContains(t, body, "\n", "%q", value)
		assert.NotContains(t, body, "\r", "%q", value)
		assert.NotContains(t, body, "\u2028", "%q", value)
		assert.NotContains(t, body, "\u2029", "%q", value)
	}
}
