package connect

import (
	"testing"

	agentdwhv1 "buf.build/gen/go/getsynq/api/protocolbuffers/go/synq/agent/dwh/v1"
	scrapperclickhouse "github.com/getsynq/dwhsupport/scrapper/clickhouse"
	"github.com/stretchr/testify/assert"
)

func TestClickhouseScrapperConf(t *testing.T) {
	got := clickhouseScrapperConf(&agentdwhv1.ClickhouseConf{
		Host:         "clickhouse.example",
		Port:         9440,
		Username:     "scraper",
		Password:     "sekret",
		InstanceName: "prod",
		Database:     "analytics",
		Settings:     map[string]string{"max_execution_time": "300"},
		Cluster: &agentdwhv1.ClickhouseClusterConf{
			Mode: agentdwhv1.ClickhouseClusterMode_CLICKHOUSE_CLUSTER_MODE_ALL_REPLICAS,
			Name: "analytics_cluster",
		},
	})

	assert.Equal(t, "clickhouse.example", got.Hostname)
	assert.Equal(t, 9440, got.Port)
	assert.Equal(t, "scraper", got.Username)
	assert.Equal(t, "sekret", got.Password)
	// The two settings the connection spells separately stay separate: the name the
	// scrape publishes under, and the database the connection opens with.
	assert.Equal(t, "prod", got.InstanceName)
	assert.Equal(t, "analytics", got.DefaultDatabase)
	assert.False(t, got.NoSsl)
	assert.Equal(t, map[string]string{"max_execution_time": "300"}, got.Settings)
	assert.Equal(t, scrapperclickhouse.ClusterConf{Name: "analytics_cluster"}, got.Cluster)
}

// A connection that names neither setting reads across the default cluster, which
// is what every ClickHouse connection did before either was configurable.
func TestClickhouseScrapperConf_Unconfigured(t *testing.T) {
	got := clickhouseScrapperConf(&agentdwhv1.ClickhouseConf{Host: "clickhouse.example"})

	assert.Nil(t, got.Settings)
	assert.Equal(t, scrapperclickhouse.ClusterConf{}, got.Cluster)
	assert.False(t, got.Cluster.SingleNode)
	// No name configured leaves the scrape to publish under the connection host,
	// the same identity a Coalesce Quality-hosted connection falls back to.
	assert.Empty(t, got.InstanceName)
	assert.Empty(t, got.DefaultDatabase)
}

func TestClickhouseClusterConf(t *testing.T) {
	for name, tc := range map[string]struct {
		conf *agentdwhv1.ClickhouseClusterConf
		want scrapperclickhouse.ClusterConf
	}{
		"absent": {
			conf: nil,
			want: scrapperclickhouse.ClusterConf{},
		},
		"mode unset fans out across the default cluster": {
			conf: &agentdwhv1.ClickhouseClusterConf{Name: "named"},
			want: scrapperclickhouse.ClusterConf{Name: "named"},
		},
		"all replicas": {
			conf: &agentdwhv1.ClickhouseClusterConf{
				Mode: agentdwhv1.ClickhouseClusterMode_CLICKHOUSE_CLUSTER_MODE_ALL_REPLICAS,
				Name: "named",
			},
			want: scrapperclickhouse.ClusterConf{Name: "named"},
		},
		"single node drops the cluster name": {
			conf: &agentdwhv1.ClickhouseClusterConf{
				Mode: agentdwhv1.ClickhouseClusterMode_CLICKHOUSE_CLUSTER_MODE_SINGLE_NODE,
				Name: "named",
			},
			want: scrapperclickhouse.ClusterConf{SingleNode: true},
		},
	} {
		t.Run(name, func(t *testing.T) {
			got := clickhouseClusterConf(tc.conf)
			assert.Equal(t, tc.want, got)
			assert.NoError(t, got.Validate())
		})
	}
}
