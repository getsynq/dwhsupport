package trino

import (
	"context"

	"github.com/getsynq/dwhsupport/scrapper"
)

// QueryPlatformUsers is unsupported: Trino authenticates through a pluggable
// authenticator (LDAP, OAuth, password file, Starburst's own) and keeps no
// catalog of users it could list. system.runtime.queries names who ran a
// query, but only while the coordinator still holds it.
func (e *TrinoScrapper) QueryPlatformUsers(ctx context.Context) (*scrapper.PlatformUsers, error) {
	return nil, scrapper.ErrUnsupported
}
