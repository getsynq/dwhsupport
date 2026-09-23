package db2

import (
	"context"
	"os"
	"testing"

	dwhexecdb2 "github.com/getsynq/dwhsupport/exec/db2"
	"github.com/getsynq/dwhsupport/testenv"
	"github.com/joho/godotenv"
)

func TestMain(m *testing.M) {
	_ = godotenv.Load("../../.env")

	os.Exit(m.Run())
}

// newDb2ScrapperFromEnv connects to the dwhtesting Db2 (lib/db2 in the cloud
// repo's dev-infra/dwhtesting) over Twingate unless DB2_* says otherwise.
func newDb2ScrapperFromEnv(ctx context.Context) (*Db2Scrapper, error) {
	return NewDb2Scrapper(ctx, &Db2ScrapperConf{
		Db2Conf: dwhexecdb2.Db2Conf{
			Hostname: testenv.EnvOrDefault("DB2_HOSTNAME", "db2.dwh-testing.svc.cluster.local"),
			Port:     testenv.EnvOrDefaultInt("DB2_PORT", 50000),
			Database: testenv.EnvOrDefault("DB2_DATABASE", "TESTDB"),
			User:     testenv.EnvOrDefault("DB2_USER", "synq_reader"),
			Password: testenv.EnvOrDefault("DB2_PASSWORD", "SynqTest1"),
		},
	})
}

func skipInCI(t *testing.T) {
	t.Helper()
	if os.Getenv("CI") != "" {
		t.Skip("Skipping Db2 tests in CI")
	}
}
