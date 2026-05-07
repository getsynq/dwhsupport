package snowflake

import (
	"os"
	"testing"
	"time"

	"github.com/snowflakedb/gosnowflake"
	"github.com/stretchr/testify/suite"
)

type SnowflakeAuthTypeTestSuite struct {
	suite.Suite
}

func TestSnowflakeAuthTypeTestSuite(t *testing.T) {
	suite.Run(t, new(SnowflakeAuthTypeTestSuite))
}

func (s *SnowflakeAuthTypeTestSuite) TestBuildConfig_ExternalBrowser() {
	conf := &SnowflakeConf{
		Account:   "myaccount",
		User:      "user@example.com",
		AuthType:  "externalbrowser",
		Databases: []string{"DB1", "DB2"},
	}
	c, err := buildSnowflakeConfig(conf)
	s.Require().NoError(err)

	s.Equal(gosnowflake.AuthTypeExternalBrowser, c.Authenticator)
	s.Equal(120*time.Second, c.ExternalBrowserTimeout)
	s.Equal(gosnowflake.ConfigBoolTrue, c.ClientStoreTemporaryCredential)
	s.Equal(gosnowflake.ConfigBoolFalse, c.DisableConsoleLogin)
	// SSO connections should not set a default database — the user's role
	// may not have access to the workspace integration's databases.
	s.Empty(c.Database)
}

func (s *SnowflakeAuthTypeTestSuite) TestBuildConfig_ExternalBrowserCaseInsensitive() {
	testCases := []string{"ExternalBrowser", "EXTERNALBROWSER", "externalBrowser", "ExTeRnAlBrOwSeR"}

	for _, authType := range testCases {
		conf := &SnowflakeConf{
			Account:  "myaccount",
			User:     "user@example.com",
			AuthType: authType,
		}
		c, err := buildSnowflakeConfig(conf)
		s.Require().NoError(err)

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
	c, err := buildSnowflakeConfig(conf)
	s.Require().NoError(err)

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
	c, err := buildSnowflakeConfig(conf)
	s.Require().NoError(err)

	// Unrecognized auth type should fall back to default behavior
	s.NotEqual(gosnowflake.AuthTypeExternalBrowser, c.Authenticator)
	s.Equal(gosnowflake.ConfigBoolTrue, c.DisableConsoleLogin)
}

func (s *SnowflakeAuthTypeTestSuite) TestBuildConfig_OAuthToken() {
	conf := &SnowflakeConf{
		Account:   "myaccount",
		Token:     "my-oauth-access-token",
		Role:      "PUBLIC",
		Databases: []string{"DB1", "DB2"},
	}
	c, err := buildSnowflakeConfig(conf)
	s.Require().NoError(err)

	s.Equal(gosnowflake.AuthTypeOAuth, c.Authenticator)
	s.Equal("my-oauth-access-token", c.Token)
	s.Equal("PUBLIC", c.Role)
	s.Equal(gosnowflake.ConfigBoolTrue, c.DisableConsoleLogin)
	// OAuth connections should not set a default database — the user's role
	// may not have access to the workspace integration's databases.
	s.Empty(c.Database)
}

func (s *SnowflakeAuthTypeTestSuite) TestBuildConfig_PasswordSetsDefaultDatabase() {
	conf := &SnowflakeConf{
		Account:   "myaccount",
		User:      "svc_user",
		Password:  "password",
		Databases: []string{"DB1", "DB2"},
	}
	c, err := buildSnowflakeConfig(conf)
	s.Require().NoError(err)

	s.Equal("DB1", c.Database)
}

func (s *SnowflakeAuthTypeTestSuite) TestBuildConfig_OAuthTokenTakesPrecedence() {
	// When both Token and Password are set, Token should win
	conf := &SnowflakeConf{
		Account:  "myaccount",
		User:     "user@example.com",
		Password: "password123",
		Token:    "my-oauth-access-token",
	}
	c, err := buildSnowflakeConfig(conf)
	s.Require().NoError(err)

	s.Equal(gosnowflake.AuthTypeOAuth, c.Authenticator)
	s.Equal("my-oauth-access-token", c.Token)
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
	c, err := buildSnowflakeConfig(conf)
	s.Require().NoError(err)

	s.Equal(gosnowflake.AuthTypeJwt, c.Authenticator)
	s.NotNil(c.PrivateKey)
	s.Equal(gosnowflake.ConfigBoolTrue, c.DisableConsoleLogin)
}

// Regression test: an unencrypted PEM combined with a non-empty passphrase
// (e.g. browser-autofilled into the form) used to silently fall through to
// password auth and surface as "260002: password is empty" from the driver.
// buildSnowflakeConfig must surface the parse error directly.
func (s *SnowflakeAuthTypeTestSuite) TestBuildConfig_UnencryptedPrivateKeyWithPassphrase_ReturnsError() {
	keyBytes, err := os.ReadFile("test_rsa_key_unencrypted.pem")
	s.Require().NoError(err)

	conf := &SnowflakeConf{
		Account:              "myaccount",
		User:                 "svc_user",
		PrivateKey:           keyBytes,
		PrivateKeyPassphrase: "browser-autofilled-password",
	}
	c, err := buildSnowflakeConfig(conf)

	s.Require().Error(err)
	s.Nil(c)
	s.Contains(err.Error(), "passphrase provided but private key is not encrypted")
}

// Regression test: a malformed PEM must not silently fall through to password
// auth either.
func (s *SnowflakeAuthTypeTestSuite) TestBuildConfig_InvalidPrivateKey_ReturnsError() {
	conf := &SnowflakeConf{
		Account:    "myaccount",
		User:       "svc_user",
		PrivateKey: []byte("not a valid PEM"),
	}
	c, err := buildSnowflakeConfig(conf)

	s.Require().Error(err)
	s.Nil(c)
}
