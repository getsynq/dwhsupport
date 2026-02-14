package snowflake

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type SnowflakeCleanAccountNameTestSuite struct {
	suite.Suite
}

func TestSnowflakeCleanAccountNameTestSuite(t *testing.T) {
	suite.Run(t, new(SnowflakeCleanAccountNameTestSuite))
}

func (s *SnowflakeCleanAccountNameTestSuite) TestCleanAccountName_PlainAccount() {
	result := cleanAccountName("myaccount")
	s.Equal("myaccount", result)
}

func (s *SnowflakeCleanAccountNameTestSuite) TestCleanAccountName_AccountWithSuffix() {
	result := cleanAccountName("myaccount.snowflakecomputing.com")
	s.Equal("myaccount", result)
}

func (s *SnowflakeCleanAccountNameTestSuite) TestCleanAccountName_HTTPSUrl() {
	result := cleanAccountName("https://myaccount.snowflakecomputing.com")
	s.Equal("myaccount", result)
}

func (s *SnowflakeCleanAccountNameTestSuite) TestCleanAccountName_HTTPSUrlWithTrailingSlash() {
	result := cleanAccountName("https://myaccount.snowflakecomputing.com/")
	s.Equal("myaccount", result)
}

func (s *SnowflakeCleanAccountNameTestSuite) TestCleanAccountName_HTTPUrl() {
	result := cleanAccountName("http://myaccount.snowflakecomputing.com")
	s.Equal("myaccount", result)
}

func (s *SnowflakeCleanAccountNameTestSuite) TestCleanAccountName_AccountWithRegion() {
	result := cleanAccountName("myaccount.us-east-1")
	s.Equal("myaccount.us-east-1", result)
}

func (s *SnowflakeCleanAccountNameTestSuite) TestCleanAccountName_AccountWithRegionAndSuffix() {
	result := cleanAccountName("myaccount.us-east-1.snowflakecomputing.com")
	s.Equal("myaccount.us-east-1", result)
}

func (s *SnowflakeCleanAccountNameTestSuite) TestCleanAccountName_URLWithRegionAndSuffix() {
	result := cleanAccountName("https://myaccount.us-east-1.snowflakecomputing.com")
	s.Equal("myaccount.us-east-1", result)
}

func (s *SnowflakeCleanAccountNameTestSuite) TestCleanAccountName_WithWhitespace() {
	result := cleanAccountName("  myaccount.snowflakecomputing.com  ")
	s.Equal("myaccount", result)
}

func (s *SnowflakeCleanAccountNameTestSuite) TestCleanAccountName_EmptyString() {
	result := cleanAccountName("")
	s.Equal("", result)
}

func (s *SnowflakeCleanAccountNameTestSuite) TestCleanAccountName_InvalidURL() {
	// Invalid URLs should still work by falling back to string processing
	result := cleanAccountName("https://")
	s.Equal("https:/", result)
}
