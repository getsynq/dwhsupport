package athena

import (
	"context"

	"github.com/getsynq/dwhsupport/scrapper"
)

// QueryPlatformUsers is unsupported: Athena has no users of its own. Whoever
// runs a query is an AWS IAM principal, and listing those is an IAM question
// across the whole AWS account, not one the workgroup can answer.
func (e *AthenaScrapper) QueryPlatformUsers(ctx context.Context) (*scrapper.PlatformUsers, error) {
	return nil, scrapper.ErrUnsupported
}
