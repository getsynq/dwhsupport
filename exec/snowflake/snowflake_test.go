package snowflake

import (
	"os"
	"testing"
	"time"

	"github.com/snowflakedb/gosnowflake"
	"github.com/stretchr/testify/suite"
)

type SnowflakePrivateKeyTestSuite struct {
	suite.Suite
}

func TestSnowflakePrivateKeyTestSuite(t *testing.T) {
	suite.Run(t, new(SnowflakePrivateKeyTestSuite))
}

func (s *SnowflakePrivateKeyTestSuite) TestParseUnencryptedPKCS8PrivateKey() {
	keyBytes, err := os.ReadFile("test_rsa_key_unencrypted.pem")
	s.Require().NoError(err)

	privKey, err := parsePrivateKey(keyBytes, "")
	s.Require().NoError(err)
	s.NotNil(privKey)
	s.NotNil(privKey.PublicKey)
}

func (s *SnowflakePrivateKeyTestSuite) TestParseEncryptedPKCS8PrivateKeyWithPassphrase() {
	keyBytes, err := os.ReadFile("test_rsa_key_encrypted.pem")
	s.Require().NoError(err)

	privKey, err := parsePrivateKey(keyBytes, "testpassphrase")
	s.Require().NoError(err)
	s.NotNil(privKey)
	s.NotNil(privKey.PublicKey)
}

func (s *SnowflakePrivateKeyTestSuite) TestParseEncryptedPKCS8PrivateKeyWithWrongPassphrase() {
	keyBytes, err := os.ReadFile("test_rsa_key_encrypted.pem")
	s.Require().NoError(err)

	_, err = parsePrivateKey(keyBytes, "wrongpassphrase")
	s.Error(err)
	s.Contains(err.Error(), "failed to parse encrypted private key")
}

func (s *SnowflakePrivateKeyTestSuite) TestParseEncryptedPKCS8PrivateKeyWithoutPassphrase() {
	keyBytes, err := os.ReadFile("test_rsa_key_encrypted.pem")
	s.Require().NoError(err)

	_, err = parsePrivateKey(keyBytes, "")
	s.Error(err)
	s.Contains(err.Error(), "encrypted private key is provided but no passphrase is set")
}

func (s *SnowflakePrivateKeyTestSuite) TestParseInvalidPEMData() {
	_, err := parsePrivateKey([]byte("not a valid pem"), "")
	s.Error(err)
	s.Contains(err.Error(), "failed to decode PEM block")
}

func (s *SnowflakePrivateKeyTestSuite) TestParseEmptyPEMData() {
	_, err := parsePrivateKey([]byte(""), "")
	s.Error(err)
	s.Contains(err.Error(), "failed to decode PEM block")
}

func (s *SnowflakePrivateKeyTestSuite) TestParseUnencryptedPKCS8PrivateKeyWithPassphrase() {
	keyBytes, err := os.ReadFile("test_rsa_key_unencrypted.pem")
	s.Require().NoError(err)

	_, err = parsePrivateKey(keyBytes, "somepassphrase")
	s.Error(err)
	s.Contains(err.Error(), "passphrase provided but private key is not encrypted")
}

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

type SnowflakeAuthTypeTestSuite struct {
	suite.Suite
}

func TestSnowflakeAuthTypeTestSuite(t *testing.T) {
	suite.Run(t, new(SnowflakeAuthTypeTestSuite))
}

func (s *SnowflakeAuthTypeTestSuite) TestBuildConfig_ExternalBrowser() {
	conf := &SnowflakeConf{
		Account:  "myaccount",
		User:     "user@example.com",
		AuthType: "externalbrowser",
	}
	c := buildSnowflakeConfig(conf)

	s.Equal(gosnowflake.AuthTypeExternalBrowser, c.Authenticator)
	s.Equal(120*time.Second, c.ExternalBrowserTimeout)
	s.Equal(gosnowflake.ConfigBoolTrue, c.ClientStoreTemporaryCredential)
	s.Equal(gosnowflake.ConfigBoolFalse, c.DisableConsoleLogin)
}

func (s *SnowflakeAuthTypeTestSuite) TestBuildConfig_ExternalBrowserCaseInsensitive() {
	testCases := []string{"ExternalBrowser", "EXTERNALBROWSER", "externalBrowser", "ExTeRnAlBrOwSeR"}

	for _, authType := range testCases {
		conf := &SnowflakeConf{
			Account:  "myaccount",
			User:     "user@example.com",
			AuthType: authType,
		}
		c := buildSnowflakeConfig(conf)

		s.Equal(gosnowflake.AuthTypeExternalBrowser, c.Authenticator, "AuthType %q should be recognized as externalbrowser", authType)
	}
}

func (s *SnowflakeAuthTypeTestSuite) TestBuildConfig_DefaultAuthType_Empty() {
	conf := &SnowflakeConf{
		Account:  "myaccount",
		User:     "user@example.com",
		Password: "password123",
		AuthType: "",
	}
	c := buildSnowflakeConfig(conf)

	// Default auth type should not set external browser authenticator
	s.NotEqual(gosnowflake.AuthTypeExternalBrowser, c.Authenticator)
	s.Equal(gosnowflake.ConfigBoolTrue, c.DisableConsoleLogin)
}

func (s *SnowflakeAuthTypeTestSuite) TestBuildConfig_DefaultAuthType_Unrecognized() {
	conf := &SnowflakeConf{
		Account:  "myaccount",
		User:     "user@example.com",
		Password: "password123",
		AuthType: "unknowntype",
	}
	c := buildSnowflakeConfig(conf)

	// Unrecognized auth type should fall back to default behavior
	s.NotEqual(gosnowflake.AuthTypeExternalBrowser, c.Authenticator)
	s.Equal(gosnowflake.ConfigBoolTrue, c.DisableConsoleLogin)
}

func (s *SnowflakeAuthTypeTestSuite) TestBuildConfig_DefaultAuthType_WithPrivateKey() {
	keyBytes, err := os.ReadFile("test_rsa_key_unencrypted.pem")
	s.Require().NoError(err)

	conf := &SnowflakeConf{
		Account:    "myaccount",
		User:       "user@example.com",
		PrivateKey: keyBytes,
		AuthType:   "",
	}
	c := buildSnowflakeConfig(conf)

	s.Equal(gosnowflake.AuthTypeJwt, c.Authenticator)
	s.NotNil(c.PrivateKey)
	s.Equal(gosnowflake.ConfigBoolTrue, c.DisableConsoleLogin)
}
