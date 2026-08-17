package clickhouse

import (
	"fmt"
	"regexp"
)

// DefaultClusterName is the cluster the scrapper reads system tables through when
// the configuration does not name one. ClickHouse Cloud always defines it.
const DefaultClusterName = "default"

// ClusterConf selects how the scrapper reads ClickHouse system tables.
//
// The zero value fans out with clusterAllReplicas(DefaultClusterName, …): system
// tables are per-node, so a metadata read has to see every replica rather than
// whichever one answered.
type ClusterConf struct {
	// SingleNode reads system tables directly on the connected node instead of
	// fanning out across a cluster. It is the only thing that works on an install
	// whose remote_servers defines no cluster, and it removes the need for the
	// REMOTE privilege — but on an install that does have replicas it reports the
	// metadata of one node, so it has to be chosen deliberately rather than
	// inherited from an unset field.
	SingleNode bool
	// Name is the cluster passed to clusterAllReplicas. Empty means
	// DefaultClusterName. Ignored when SingleNode is set.
	Name string
}

// clusterNamePattern accepts what ClickHouse accepts: a cluster name is the name
// of an XML element under remote_servers in the server configuration.
var clusterNamePattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_.\-]*$`)

func (c ClusterConf) Validate() error {
	if c.SingleNode {
		if c.Name != "" {
			return fmt.Errorf("clickhouse: cluster name %q is set alongside single-node reads", c.Name)
		}
		return nil
	}
	if c.Name != "" && !clusterNamePattern.MatchString(c.Name) {
		return fmt.Errorf("clickhouse: %q is not a valid cluster name", c.Name)
	}
	return nil
}

// clusterName is the cluster the fan-out reads through.
func (c ClusterConf) clusterName() string {
	if c.Name == "" {
		return DefaultClusterName
	}
	return c.Name
}

// systemTableRefPattern matches the fanned-out system table references the query
// files are written with. The queries are stored in the form that runs as-is
// against ClickHouse Cloud, so they stay readable and pasteable; resolveSystemTables
// rewrites them for everything else.
var systemTableRefPattern = regexp.MustCompile(`clusterAllReplicas\(\s*default\s*,\s*(system\.[A-Za-z0-9_]+)\s*\)`)

// resolveSystemTables rewrites every system table reference in a query to match the
// cluster configuration. Every query the scrapper sends has to go through this —
// a reference left behind reads the hardcoded default cluster and fails on any
// install that does not have one.
func (c ClusterConf) resolveSystemTables(sql string) string {
	if c.SingleNode {
		return systemTableRefPattern.ReplaceAllString(sql, "$1")
	}
	return systemTableRefPattern.ReplaceAllString(sql, fmt.Sprintf("clusterAllReplicas('%s', $1)", c.clusterName()))
}
