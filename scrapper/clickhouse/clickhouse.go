package clickhouse

import (
	"context"
	_ "embed"
	"fmt"
	"slices"
	"strings"

	dwhexecclickhouse "github.com/getsynq/dwhsupport/exec/clickhouse"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/sqldialect"
)

type ClickhouseScrapperConf struct {
	dwhexecclickhouse.ClickhouseConf
	// InstanceName is the name this ClickHouse is published under. ClickHouse has
	// no container above a database, so something has to name the service itself;
	// empty leaves that to the connection host. It is not a database and does not
	// restrict what is scraped.
	InstanceName string
	// Cluster selects how system tables are read. See ClusterConf.
	Cluster ClusterConf
}

var _ scrapper.Scrapper = &ClickhouseScrapper{}

type ClickhouseScrapper struct {
	conf     ClickhouseScrapperConf
	executor *dwhexecclickhouse.ClickhouseExecutor
}

func NewClickhouseScrapper(ctx context.Context, conf ClickhouseScrapperConf) (*ClickhouseScrapper, error) {
	if err := conf.Cluster.Validate(); err != nil {
		return nil, err
	}

	executor, err := dwhexecclickhouse.NewClickhouseExecutor(ctx, &conf.ClickhouseConf)
	if err != nil {
		return nil, err
	}

	return &ClickhouseScrapper{executor: executor, conf: conf}, nil
}

// systemTablesSql prepares a query for the configured cluster. Every query the
// scrapper sends against a system table goes through it.
func (e *ClickhouseScrapper) systemTablesSql(sql string) string {
	return e.conf.Cluster.resolveSystemTables(sql)
}

// rowIdentity is what every scrapped row is keyed on: the endpoint the metadata
// was read from, and the name the workspace publishes this ClickHouse under —
// empty unless one was configured, and a ClickHouse database below it is a
// schema, not a container.
//
// The two travel in separate fields because only one of them is an address. A
// host has many spellings that name one service — a URL, a port, a
// private-connectivity endpoint — and the reader reduces them to one; a
// configured name has exactly one spelling and passes through as written.
func (e *ClickhouseScrapper) rowIdentity() (instance, name string) {
	return e.conf.Hostname, e.conf.InstanceName
}

func (e *ClickhouseScrapper) IsPermissionError(err error) bool {
	return dwhexecclickhouse.IsPermissionError(err)
}

func (e *ClickhouseScrapper) Capabilities() scrapper.Capabilities {
	return scrapper.Capabilities{
		EstimateQuery: scrapper.EstimateQueryCapability{Supported: true, ReportsRows: true},
	}
}

func (e *ClickhouseScrapper) DialectType() string {
	return "clickhouse"
}

func (e *ClickhouseScrapper) SqlDialect() sqldialect.Dialect {
	return sqldialect.NewClickHouseDialect()
}

func (e *ClickhouseScrapper) Executor() *dwhexecclickhouse.ClickhouseExecutor {
	return e.executor
}

// ValidateConfiguration reports a configured cluster this server does not have,
// which is the difference between the warehouse owner learning about it here and
// learning about it as a CLUSTER_DOESNT_EXIST on the first catalog fetch.
func (e *ClickhouseScrapper) ValidateConfiguration(ctx context.Context) ([]string, error) {
	if e.conf.Cluster.SingleNode {
		return nil, nil
	}

	wanted := e.conf.Cluster.clusterName()

	var available []string
	if err := e.executor.Select(ctx, &available, `SELECT DISTINCT cluster FROM system.clusters ORDER BY cluster`); err != nil {
		// Reading system.clusters is not a grant we ask for, so failing to answer
		// the question is not a reason to fail the configuration.
		return nil, nil
	}
	if slices.Contains(available, wanted) {
		return nil, nil
	}

	if len(available) == 0 {
		return []string{
			fmt.Sprintf(
				"this ClickHouse defines no cluster, so reading metadata through cluster %q will fail — configure single-node reads instead",
				wanted,
			),
		}, nil
	}
	return []string{
		fmt.Sprintf("cluster %q does not exist on this ClickHouse; it has %s", wanted, strings.Join(available, ", ")),
	}, nil
}

func (e *ClickhouseScrapper) Close() error {
	return e.executor.Close()
}
