package trino

import (
	"context"
	"os"
	"testing"

	"github.com/getsynq/dwhsupport/exec/trino"
	"github.com/stretchr/testify/suite"
)

type ValidateConfigSuite struct {
	suite.Suite
}

func TestValidateConfigSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("skipping Trino test in CI environment")
	}
	suite.Run(t, new(ValidateConfigSuite))
}

func (s *ValidateConfigSuite) newScrapperWithCatalogs(catalogs []string) *TrinoScrapper {
	ctx := context.TODO()
	conf := &trino.TrinoConf{
		User:     os.Getenv("STARBURST_USER"),
		Password: os.Getenv("STARBURST_PASSWORD"),
		Host:     "synq-free-gcp.trino.galaxy.starburst.io",
		Port:     443,
	}
	scr, err := NewTrinoScrapper(ctx, &TrinoScrapperConf{
		TrinoConf: conf,
		Catalogs:  catalogs,
	})
	s.Require().NoError(err)
	s.Require().NotNil(scr)
	return scr
}

func (s *ValidateConfigSuite) TestValidateConfiguration_AllCatalogsPresent() {
	scr := s.newScrapperWithCatalogs([]string{"iceberg_gcs"})
	defer scr.Close()
	ctx := context.TODO()
	msgs, err := scr.ValidateConfiguration(ctx)
	s.Require().NoError(err)
	s.Empty(msgs)
}

func (s *ValidateConfigSuite) TestValidateConfiguration_MissingCatalogs() {
	scr := s.newScrapperWithCatalogs([]string{"iceberg_gcs", "definitely_missing_catalog"})
	defer scr.Close()
	ctx := context.TODO()
	msgs, err := scr.ValidateConfiguration(ctx)
	s.Require().NoError(err)
	s.NotEmpty(msgs)
	s.Contains(msgs[0], "definitely_missing_catalog")
}
