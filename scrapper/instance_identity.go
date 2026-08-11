package scrapper

import "context"

// Identity function/field keys used in InstanceIdentity.Raw. Each names the
// warehouse construct that produced the value, so a caller can tell an account
// locator from an organization-qualified name without re-deriving either.
const (
	// InstanceIdentityAccountLocator is the identifier the warehouse assigned
	// the account itself (Snowflake CURRENT_ACCOUNT()).
	InstanceIdentityAccountLocator = "account_locator"
	// InstanceIdentityOrganizationName is the organization the account belongs
	// to (Snowflake CURRENT_ORGANIZATION_NAME()).
	InstanceIdentityOrganizationName = "organization_name"
	// InstanceIdentityAccountName is the account's name within its organization
	// (Snowflake CURRENT_ACCOUNT_NAME()).
	InstanceIdentityAccountName = "account_name"
	// InstanceIdentityRegion is the region identifier, which is NOT the same
	// string as the region fragment appearing in the account's hostname
	// (Snowflake CURRENT_REGION() returns AWS_US_EAST_2 for us-east-2.aws).
	InstanceIdentityRegion = "region"
)

// Host kinds used in InstanceHost.Kind.
const (
	// InstanceHostRegional is the account's region-qualified hostname, built
	// from the account locator (Snowflake SNOWFLAKE_DEPLOYMENT).
	InstanceHostRegional = "regional"
	// InstanceHostRegionless is the account's region-independent hostname,
	// built from the organization-qualified account name (Snowflake
	// SNOWFLAKE_DEPLOYMENT_REGIONLESS).
	InstanceHostRegionless = "regionless"
)

// InstanceHost is one hostname the warehouse reports for itself.
type InstanceHost struct {
	// Host is the hostname exactly as the warehouse reported it, including any
	// vendor domain suffix.
	Host string
	// Kind classifies the host, since one account is typically reachable under
	// several names that encode different identifier forms. One of the
	// InstanceHost* constants; empty when the warehouse gave no classification.
	Kind string
}

// InstanceIdentity is what a warehouse answers when asked what it calls itself.
//
// It exists because a single warehouse instance is often addressable under
// several unrelated identifiers, and which one a given tool reports is decided
// by that tool, not by us. Snowflake is the motivating case: an account has an
// account locator and an organization-qualified name, and neither is derivable
// from the other by string manipulation — the mapping lives inside Snowflake.
// Two producers can therefore normalize an account correctly and still disagree
// about its identity. Asking the warehouse is the only way to learn every form.
//
// The values are deliberately RAW: exactly what the warehouse returned, never
// parsed, cased or assembled into an identifier here. Callers own identifier
// construction and apply their own normalization, which this package must not
// try to reproduce.
type InstanceIdentity struct {
	// Raw holds each value keyed by the warehouse construct that produced it
	// (the InstanceIdentity* constants). A key is absent when the warehouse did
	// not report it; no key is guaranteed present.
	Raw map[string]string
	// Hosts are the hostnames the warehouse reports for the instance itself.
	// Hostnames of unrelated infrastructure the instance happens to talk to
	// (object storage, OCSP responders) are not included.
	Hosts []InstanceHost
}

// Get returns the raw value for key, or "" when the warehouse did not report it.
func (i *InstanceIdentity) Get(key string) string {
	if i == nil {
		return ""
	}
	return i.Raw[key]
}

// InstanceIdentityQuerier is implemented by scrappers that can ask their
// warehouse what it calls itself. It is optional: most platforms have a single
// identifier form and need nothing here. Reach it through As, so the capability
// survives being wrapped by decorators:
//
//	if q, ok := scrapper.As[scrapper.InstanceIdentityQuerier](s); ok { ... }
type InstanceIdentityQuerier interface {
	// QueryInstanceIdentity returns every form of its own identity the
	// warehouse will report. It returns ErrUnsupported when the platform has
	// no such concept.
	QueryInstanceIdentity(ctx context.Context) (*InstanceIdentity, error)
}
