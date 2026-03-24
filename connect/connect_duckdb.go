//go:build cgo

package connect

import (
	"context"

	agentdwhv1 "buf.build/gen/go/getsynq/api/protocolbuffers/go/synq/agent/dwh/v1"
	dwhexecduckdb "github.com/getsynq/dwhsupport/exec/duckdb"
	"github.com/getsynq/dwhsupport/scrapper"
	scrapperduckdb "github.com/getsynq/dwhsupport/scrapper/duckdb"
)

func init() {
	duckDBConnector = func(ctx context.Context, conf *agentdwhv1.DuckDBConf) (scrapper.Scrapper, error) {
		if conf.GetMotherduckAccount() != "" {
			return scrapperduckdb.NewDuckDBScrapper(ctx, &scrapperduckdb.DuckDBScapperConf{
				DuckDBConf: dwhexecduckdb.DuckDBConf{
					MotherduckAccount: conf.GetMotherduckAccount(),
					MotherduckToken:   conf.GetMotherduckToken(),
				},
			})
		}
		return scrapperduckdb.NewLocalDuckDBScrapper(ctx, conf.GetDatabase(), conf.GetDatabase())
	}
}
