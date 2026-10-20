package scrapper

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsUnresolvedPolicyTag(t *testing.T) {
	const taxonomy = "projects/governance/locations/eu/taxonomies/100"
	require.True(t, Tag{TagName: taxonomy, TagValue: taxonomy + "/policyTags/1", Kind: TagKindPolicyTag}.IsUnresolvedPolicyTag())
	require.False(t, Tag{TagName: "PII", TagValue: "High:Email", Kind: TagKindPolicyTag}.IsUnresolvedPolicyTag(), "resolved")
	require.False(t, Tag{TagName: taxonomy, Kind: TagKindLabel}.IsUnresolvedPolicyTag(), "a label is never a policy tag")
}
