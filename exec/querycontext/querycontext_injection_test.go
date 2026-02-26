package querycontext

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSQLInjectionViaCommentClose demonstrates that a malicious value containing
// "*/" cannot escape the SQL comment and inject arbitrary SQL.
func TestSQLInjectionViaCommentClose(t *testing.T) {
	db, err := sqlx.Open("duckdb", "")
	require.NoError(t, err)
	defer db.Close()

	// Create a table so we can verify injection doesn't execute extra statements.
	_, err = db.Exec("CREATE TABLE injection_test (id INTEGER)")
	require.NoError(t, err)

	// A malicious value that closes the comment and injects SQL.
	maliciousValue := "evil */ ; DROP TABLE injection_test; /*"
	qc := QueryContext{"source": maliciousValue}

	// Build the query with the malicious context appended.
	baseSQL := "SELECT 1 AS val"
	ctx := WithQueryContext(context.Background(), qc)
	fullSQL := AppendSQLComment(ctx, baseSQL)

	t.Logf("Generated SQL: %s", fullSQL)

	// The query should execute safely — the comment is properly sanitized.
	_, _ = db.Exec(fullSQL)

	// Verify the table still exists — if injection succeeded, this would fail.
	var count int
	err = db.Get(&count, "SELECT COUNT(*) FROM injection_test")
	require.NoError(t, err, "injection_test table was dropped — SQL injection succeeded!")
	assert.Equal(t, 0, count)
}

// TestSQLInjectionViaKey tests injection through the key (not just value).
func TestSQLInjectionViaKey(t *testing.T) {
	db, err := sqlx.Open("duckdb", "")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE injection_test2 (id INTEGER)")
	require.NoError(t, err)

	maliciousKey := "evil */ ; DROP TABLE injection_test2; /*"
	qc := QueryContext{maliciousKey: "val"}

	baseSQL := "SELECT 1 AS val"
	ctx := WithQueryContext(context.Background(), qc)
	fullSQL := AppendSQLComment(ctx, baseSQL)

	t.Logf("Generated SQL: %s", fullSQL)

	_, _ = db.Exec(fullSQL)

	var count int
	err = db.Get(&count, "SELECT COUNT(*) FROM injection_test2")
	require.NoError(t, err, "injection_test2 table was dropped — SQL injection succeeded!")
	assert.Equal(t, 0, count)
}

// TestCommentCloseInValueIsSanitized verifies that */ in values is escaped.
func TestCommentCloseInValueIsSanitized(t *testing.T) {
	qc := QueryContext{"source": "contains */ close"}
	comment := qc.FormatAsSQLComment()

	t.Logf("Comment: %s", comment)

	// The comment must not contain an unescaped */ (other than the closing one).
	assert.NotContains(t, comment[:len(comment)-2], "*/",
		"comment body should not contain */ sequence")
}

// TestCommentCloseInKeyIsSanitized verifies that */ in keys is escaped.
func TestCommentCloseInKeyIsSanitized(t *testing.T) {
	qc := QueryContext{"key*/end": "value"}
	comment := qc.FormatAsSQLComment()

	t.Logf("Comment: %s", comment)

	assert.NotContains(t, comment[:len(comment)-2], "*/",
		"comment body should not contain */ sequence")
}

// TestSanitizedCommentIsValidJSON verifies the sanitized comment body is still
// valid JSON that decodes back to the original key-value pairs.
func TestSanitizedCommentIsValidJSON(t *testing.T) {
	qc := QueryContext{
		"source":  "synq",
		"evil":    "val*/ue",
		"key*/ok": "normal",
	}

	comment := qc.FormatAsSQLComment()
	t.Logf("Comment: %s", comment)

	// Extract JSON from between /* and */
	jsonStr := strings.TrimPrefix(comment, " /* ")
	jsonStr = strings.TrimSuffix(jsonStr, " */")

	var parsed map[string]string
	err := json.Unmarshal([]byte(jsonStr), &parsed)
	require.NoError(t, err, "sanitized comment should contain valid JSON")

	assert.Equal(t, "synq", parsed["source"])
	assert.Equal(t, "val*/ue", parsed["evil"])
	assert.Equal(t, "normal", parsed["key*/ok"])
}
