package fabric

import (
	"encoding/base32"
	"fmt"
	"strings"

	"github.com/pkg/errors"
)

// HostIdentity is the tenant and workspace identity encoded in a Fabric SQL
// analytics endpoint hostname.
type HostIdentity struct {
	// TenantID is the Entra tenant GUID.
	TenantID string
	// WorkspaceID is the Fabric workspace GUID — the Fabric REST API key for
	// /v1/workspaces/{id}/ items (warehouses, pipelines, notebooks, jobs …).
	WorkspaceID string
}

// ParseHostIdentity decodes the tenant and workspace GUIDs from a Fabric SQL
// analytics endpoint hostname of the form
//
//	<base32(tenant)>-<base32(workspace)>.datawarehouse.fabric.microsoft.com
//
// Each label is the Base32 (RFC 4648, lower-cased, unpadded) encoding of the 16
// GUID bytes in .NET little-endian layout. This lets callers derive the
// workspace id — the join key to Fabric REST API resources — from the connection
// host alone, with no live connection. It is why the scrapper uses the host as
// the row Instance: the host is present in every connection profile (including
// third-party ones such as dbt's `server`/`host`) and still carries the
// workspace id for API linking. Returns an error if the host is not a
// recognisable Fabric endpoint.
func ParseHostIdentity(host string) (*HostIdentity, error) {
	firstLabel, _, ok := strings.Cut(host, ".")
	if !ok {
		return nil, errors.Errorf("not a Fabric endpoint host: %q", host)
	}
	tenantLabel, workspaceLabel, ok := strings.Cut(firstLabel, "-")
	if !ok {
		return nil, errors.Errorf("not a Fabric endpoint host (missing tenant-workspace labels): %q", host)
	}

	tenant, err := decodeFabricGUID(tenantLabel)
	if err != nil {
		return nil, errors.Wrapf(err, "decoding tenant label in %q", host)
	}
	workspace, err := decodeFabricGUID(workspaceLabel)
	if err != nil {
		return nil, errors.Wrapf(err, "decoding workspace label in %q", host)
	}
	return &HostIdentity{TenantID: tenant, WorkspaceID: workspace}, nil
}

var fabricBase32 = base32.StdEncoding.WithPadding(base32.NoPadding)

func decodeFabricGUID(label string) (string, error) {
	b, err := fabricBase32.DecodeString(strings.ToUpper(label))
	if err != nil {
		return "", err
	}
	if len(b) < 16 {
		return "", errors.Errorf("decoded %d bytes, need 16", len(b))
	}
	// GUID is stored .NET little-endian: the first three groups are byte-reversed.
	return fmt.Sprintf("%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
		b[3], b[2], b[1], b[0], b[5], b[4], b[7], b[6], b[8], b[9], b[10], b[11], b[12], b[13], b[14], b[15]), nil
}
