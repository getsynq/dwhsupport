package databricks

import (
	"testing"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/stretchr/testify/require"
)

// A Databricks tag is a label: its name is the tag's own, and the catalog's and
// the schema's tags reach the table beside its own.
func TestTagsAreLabelsUnderTheirOwnName(t *testing.T) {
	lookup := map[string][]*Tags{
		"main":                  {{TagName: "domain", TagValue: "sales"}},
		"main.crm":              {{TagName: "owner", TagValue: "crm-team"}},
		"main.crm.orders":       {{TagName: "certified"}},
		"main.crm.orders.email": {{TagName: "pii", TagValue: "high"}},
	}

	require.ElementsMatch(t, []*scrapper.Tag{
		{TagName: "domain", TagValue: "sales", Kind: scrapper.TagKindLabel},
		{TagName: "owner", TagValue: "crm-team", Kind: scrapper.TagKindLabel},
		{TagName: "certified", Kind: scrapper.TagKindLabel},
	}, getTableTags(lookup, "main", "crm", "orders"))
	require.Equal(t, []*scrapper.Tag{
		{TagName: "pii", TagValue: "high", Kind: scrapper.TagKindLabel},
	}, getColumnTags(lookup, "main", "crm", "orders", "email"))
}
