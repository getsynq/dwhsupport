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
