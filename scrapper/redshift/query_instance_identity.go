package redshift

import (
	"context"
	"database/sql"
	"strings"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/pkg/errors"
)

var _ scrapper.InstanceIdentityQuerier = &RedshiftScrapper{}

// namespaceQuery reads the UUID of the namespace the connection lands on. It
// is a session function available to any user, on provisioned clusters and on
// serverless workgroups.
const namespaceQuery = `SELECT current_namespace`

// QueryInstanceIdentity asks the warehouse which namespace it serves.
//
// One cluster is reachable under several hostnames: its AWS endpoint, a
// Redshift-managed VPC endpoint, a CNAME the customer runs. Tools report
// whichever one they connect through, so two of them can name one cluster
// differently. The namespace is the same through all of them, which makes it
// the value a caller can join the hostnames on.
//
// Redshift reports no hostname for itself, so Hosts is always empty.
func (e *RedshiftScrapper) QueryInstanceIdentity(ctx context.Context) (*scrapper.InstanceIdentity, error) {
	rows, err := e.executor.QueryRows(ctx, namespaceQuery)
	if err != nil {
		if isMissingCurrentNamespace(err) {
			return nil, scrapper.ErrUnsupported
		}
		return nil, errors.Wrap(err, "failed to query redshift current_namespace")
	}
	defer rows.Close()

	var namespace sql.NullString
	if rows.Next() {
		if err := rows.Scan(&namespace); err != nil {
			return nil, errors.Wrap(err, "failed to scan redshift current_namespace")
		}
	}
	if err := rows.Err(); err != nil {
		return nil, errors.Wrap(err, "failed to read redshift current_namespace")
	}

	raw := map[string]string{}
	// An absent key means "the warehouse did not report this", which a caller
	// must be able to tell from an empty answer.
	if value := strings.TrimSpace(namespace.String); namespace.Valid && value != "" {
		raw[scrapper.InstanceIdentityNamespace] = value
	}
	return &scrapper.InstanceIdentity{Raw: raw}, nil
}

// isMissingCurrentNamespace reports whether the warehouse rejected the query
// because it has no current_namespace function, which is the case on old
// cluster versions.
func isMissingCurrentNamespace(err error) bool {
	return strings.Contains(strings.ToLower(err.Error()), "current_namespace") &&
		strings.Contains(strings.ToLower(err.Error()), "does not exist")
}
