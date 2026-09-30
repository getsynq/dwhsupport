package querylogs

// MetadataQueryTextRedacted is the Metadata key a platform sets to true when the warehouse
// withheld the statement text from the credentials that read the query log. SQL is empty on
// such a log, and this key is what tells a reader that the text exists but was not
// visible, rather than dropped on purpose (INSERT payloads, oversized text).
//
// The key travels in Metadata so it reaches consumers without a wire-format change.
const MetadataQueryTextRedacted = "query_text_redacted"

// IsTextRedacted reports whether the warehouse withheld this query's text.
func (q *QueryLog) IsTextRedacted() bool {
	if q == nil || q.Metadata == nil {
		return false
	}
	return q.Metadata.GetFields()[MetadataQueryTextRedacted].GetBoolValue()
}
