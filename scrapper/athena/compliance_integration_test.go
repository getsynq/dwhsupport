package athena

import (
	"os"
	"testing"

	dwhexecathena "github.com/getsynq/dwhsupport/exec/athena"
	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
	"github.com/getsynq/dwhsupport/testenv"
	"github.com/stretchr/testify/suite"
)

// AthenaComplianceSuite runs the generic scrapper compliance checks against a
// real AWS Athena workgroup configured via environment variables. The seed lives
// in cloud/dev-infra/dwhtesting/lib/athena/seed.sql; bootstrap with
// dwhtesting/lib/athena/bootstrap.sh and reseed with `./reseed.sh athena`.
type AthenaComplianceSuite struct {
	scrappertest.ComplianceSuite
}

func TestAthenaComplianceSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping Athena compliance tests in CI")
	}
	suite.Run(t, new(AthenaComplianceSuite))
}

func (s *AthenaComplianceSuite) SetupSuite() {
	region := testenv.EnvOrDefault("ATHENA_REGION", "eu-central-1")
	workgroup := testenv.EnvOrDefault("ATHENA_WORKGROUP", "synq-dwhtesting-wg")
	accessKeyID := os.Getenv("ATHENA_ACCESS_KEY_ID")
	secret := os.Getenv("ATHENA_SECRET_ACCESS_KEY")

	if accessKeyID == "" || secret == "" {
		s.T().Skip("ATHENA_ACCESS_KEY_ID / ATHENA_SECRET_ACCESS_KEY env vars not set")
	}

	conf := &AthenaScrapperConf{
		AthenaConf: &dwhexecathena.AthenaConf{
			Region:          region,
			Workgroup:       workgroup,
			AccessKeyID:     accessKeyID,
			SecretAccessKey: secret,
		},
	}

	sc, err := NewAthenaScrapper(s.Ctx(), conf)
	if err != nil {
		s.T().Skipf("Could not connect to Athena: %v", err)
	}
	s.Scrapper = sc
}

func (s *AthenaComplianceSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}
