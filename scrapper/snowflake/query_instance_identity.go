package snowflake

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"

	"github.com/getsynq/dwhsupport/logging"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/pkg/errors"
)

var _ scrapper.InstanceIdentityQuerier = &SnowflakeScrapper{}

// instanceIdentityQuery reads the account's own names. All four are session
// functions available to any role — no SNOWFLAKE database grant and no
// ACCOUNTADMIN needed, so this works on every integration regardless of whether
// ACCOUNT_USAGE was granted.
const instanceIdentityQuery = `SELECT CURRENT_ACCOUNT(), CURRENT_ORGANIZATION_NAME(), CURRENT_ACCOUNT_NAME(), CURRENT_REGION()`

// instanceIdentityFallbackQuery drops the organization functions, which are not
// present on every account. CURRENT_ACCOUNT and CURRENT_REGION have always
// existed, so this keeps the account locator available even when the preferred
// query is rejected outright.
const instanceIdentityFallbackQuery = `SELECT CURRENT_ACCOUNT(), CURRENT_REGION()`

// allowlistQuery reports the hostnames the account is reachable at. It is the
// only non-guessing way to obtain the account's regional hostname: assembling
// it from CURRENT_ACCOUNT and CURRENT_REGION does not work, because
// CURRENT_REGION returns a region id (GCP_EUROPE_WEST4) while the hostname
// carries a DNS fragment (europe-west4.gcp), and the mapping between them is a
// Snowflake-internal table rather than a string transform. Like the identity
// functions it is available to any role.
const allowlistQuery = `SELECT SYSTEM$ALLOWLIST()`

// allowlistHostKinds maps the SYSTEM$ALLOWLIST entry types that name the
// account itself onto our platform-neutral host kinds. The allowlist also
// reports object storage, OCSP responders and per-feature subdomains
// (SPCS_REGISTRY_REGIONLESS, SNOWPARK_CONNECT, OPENFLOW, ...); none of those is
// an account URL, so anything not listed here is dropped.
var allowlistHostKinds = map[string]string{
	"SNOWFLAKE_DEPLOYMENT":            scrapper.InstanceHostRegional,
	"SNOWFLAKE_DEPLOYMENT_REGIONLESS": scrapper.InstanceHostRegionless,
}

type allowlistEntry struct {
	Host string `json:"host"`
	Type string `json:"type"`
}

// QueryInstanceIdentity asks the account for every form of its own identity:
// the account locator, the organization-qualified name, the region, and the
// hostnames it is reachable at.
//
// One Snowflake account is addressable both as a locator (JH35950, and
// jh35950.europe-west4.gcp as a hostname) and as an organization-qualified name
// (CUUNSQR-IR70409). Neither derives from the other, so a tool reporting one
// form and a tool reporting the other describe the same warehouse under names
// that cannot be reconciled by string manipulation. This is what makes the
// round trip necessary.
//
// The hostname lookup is best-effort: a failure there still returns the
// identity functions' values rather than failing the whole probe.
func (e *SnowflakeScrapper) QueryInstanceIdentity(ctx context.Context) (*scrapper.InstanceIdentity, error) {
	raw, err := e.queryIdentityFunctions(ctx)
	if err != nil {
		return nil, err
	}

	identity := &scrapper.InstanceIdentity{Raw: raw}

	hosts, err := e.queryAllowlistHosts(ctx)
	if err != nil {
		// The account already told us its names; the hostnames only add the
		// regional form. Losing them degrades the result, it does not
		// invalidate it.
		logging.GetLogger(ctx).WithError(err).
			Warn("failed to read snowflake account hostnames from SYSTEM$ALLOWLIST, continuing without them")
	} else {
		identity.Hosts = hosts
	}

	return identity, nil
}

// queryIdentityFunctions reads the account's names, falling back to the subset
// that predates the organization functions when the preferred query is
// rejected.
func (e *SnowflakeScrapper) queryIdentityFunctions(ctx context.Context) (map[string]string, error) {
	var locator, org, account, region sql.NullString

	err := e.scanSingleRow(ctx, instanceIdentityQuery, &locator, &org, &account, &region)
	if err != nil {
		logging.GetLogger(ctx).WithError(err).
			Warn("snowflake organization identity functions unavailable, falling back to account locator only")
		locator, org, account, region = sql.NullString{}, sql.NullString{}, sql.NullString{}, sql.NullString{}
		if err := e.scanSingleRow(ctx, instanceIdentityFallbackQuery, &locator, &region); err != nil {
			return nil, errors.Wrap(err, "failed to query snowflake account identity")
		}
	}

	raw := map[string]string{}
	for key, v := range map[string]sql.NullString{
		scrapper.InstanceIdentityAccountLocator:   locator,
		scrapper.InstanceIdentityOrganizationName: org,
		scrapper.InstanceIdentityAccountName:      account,
		scrapper.InstanceIdentityRegion:           region,
	} {
		// An absent key means "the warehouse did not report this", which is
		// what a caller needs to distinguish from an empty answer.
		if value := strings.TrimSpace(v.String); v.Valid && value != "" {
			raw[key] = value
		}
	}
	return raw, nil
}

func (e *SnowflakeScrapper) queryAllowlistHosts(ctx context.Context) ([]scrapper.InstanceHost, error) {
	var payload sql.NullString
	if err := e.scanSingleRow(ctx, allowlistQuery, &payload); err != nil {
		return nil, errors.Wrap(err, "failed to query SYSTEM$ALLOWLIST")
	}
	return parseAllowlistHosts(payload.String)
}

// parseAllowlistHosts keeps the entries that name the account itself, in
// allowlist order and without duplicates.
func parseAllowlistHosts(payload string) ([]scrapper.InstanceHost, error) {
	if strings.TrimSpace(payload) == "" {
		return nil, nil
	}
	var entries []allowlistEntry
	if err := json.Unmarshal([]byte(payload), &entries); err != nil {
		return nil, errors.Wrap(err, "failed to parse SYSTEM$ALLOWLIST payload")
	}
	var hosts []scrapper.InstanceHost
	seen := map[string]bool{}
	for _, entry := range entries {
		kind, ok := allowlistHostKinds[strings.ToUpper(strings.TrimSpace(entry.Type))]
		host := strings.TrimSpace(entry.Host)
		if !ok || host == "" || seen[host] {
			continue
		}
		seen[host] = true
		hosts = append(hosts, scrapper.InstanceHost{Host: host, Kind: kind})
	}
	return hosts, nil
}

// scanSingleRow runs a query expected to return exactly one row and scans it
// into dest. A query that returns no row is an error: every caller here reads
// session functions, which always produce a row.
func (e *SnowflakeScrapper) scanSingleRow(ctx context.Context, query string, dest ...any) error {
	rows, err := e.executor.QueryRows(ctx, query)
	if err != nil {
		return err
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return err
		}
		return errors.Errorf("query returned no rows: %s", query)
	}
	if err := rows.Scan(dest...); err != nil {
		return errors.Wrapf(err, "failed to scan result of %s", query)
	}
	return rows.Err()
}
