package trino

import (
	"testing"

	"github.com/getsynq/dwhsupport/querylogs"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/reject"
	"github.com/getsynq/dwhsupport/scrapper/sanitize"
	"github.com/getsynq/dwhsupport/scrapper/scope"
	"github.com/stretchr/testify/require"
)

// Trino has no query history to read: system.runtime.queries is the coordinator's
// in-memory list, capped by query.max-history and emptied by a restart, so a fetch
// of it can never say it saw every query of a window. A caller finds query logs
// with scrapper.As through whatever decorators it wrapped the scrapper in, so the
// check goes through the same chain.
func TestTrinoDoesNotProvideQueryLogs(t *testing.T) {
	var bare scrapper.Scrapper = &TrinoScrapper{}
	wrapped := reject.NewRejectingScrapper(sanitize.NewSanitizingScrapper(scope.NewScopedScrapper(bare, nil)))

	for name, s := range map[string]scrapper.Scrapper{"bare": bare, "wrapped": wrapped} {
		t.Run(name, func(t *testing.T) {
			_, ok := scrapper.As[querylogs.QueryLogsProvider](s)
			require.False(t, ok, "Trino must not offer query logs")
		})
	}
}
