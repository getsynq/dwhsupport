package mssql

import (
	"context"
	"net/url"
	"testing"

	"github.com/stretchr/testify/suite"
)

type IntegratedAuthSuite struct {
	suite.Suite
}

func TestIntegratedAuthSuite(t *testing.T) {
	suite.Run(t, new(IntegratedAuthSuite))
}

func (s *IntegratedAuthSuite) TestCredCacheFromEnvFile() {
	cache, err := krb5CredCacheFile(env(map[string]string{"KRB5CCNAME": "/home/me/.krb5cc"}), 501, existing("/home/me/.krb5cc"))
	s.Require().NoError(err)
	s.Equal("/home/me/.krb5cc", cache)
}

func (s *IntegratedAuthSuite) TestCredCacheFromEnvFilePrefix() {
	cache, err := krb5CredCacheFile(env(map[string]string{"KRB5CCNAME": "FILE:/tmp/krb5cc_501"}), 501, existing("/tmp/krb5cc_501"))
	s.Require().NoError(err)
	s.Equal("/tmp/krb5cc_501", cache)
}

func (s *IntegratedAuthSuite) TestCredCacheDefaultsToPerUserFile() {
	cache, err := krb5CredCacheFile(env(nil), 1000, existing("/tmp/krb5cc_1000"))
	s.Require().NoError(err)
	s.Equal("/tmp/krb5cc_1000", cache)
}

func (s *IntegratedAuthSuite) TestCredCacheMissingDefault() {
	_, err := krb5CredCacheFile(env(nil), 1000, existing())
	s.Require().Error(err)
	s.Contains(err.Error(), "no Kerberos ticket found")
	s.Contains(err.Error(), "kinit -c FILE:/tmp/krb5cc_1000")
}

func (s *IntegratedAuthSuite) TestCredCacheMissingFromEnv() {
	_, err := krb5CredCacheFile(env(map[string]string{"KRB5CCNAME": "FILE:/tmp/gone"}), 1000, existing())
	s.Require().Error(err)
	s.Contains(err.Error(), "/tmp/gone")
}

func (s *IntegratedAuthSuite) TestCredCacheRejectsNonFileCaches() {
	for _, cache := range []string{"API:ABCDEF-1234", "KCM:1000", "KEYRING:persistent:1000", "DIR:/run/user/1000/krb5cc"} {
		_, err := krb5CredCacheFile(env(map[string]string{"KRB5CCNAME": cache}), 1000, existing("/run/user/1000/krb5cc"))
		s.Require().Error(err, cache)
		s.Contains(err.Error(), "is not a file credential cache", cache)
	}
}

func (s *IntegratedAuthSuite) TestConnectionStringWithoutIntegratedAuthHasNoAuthenticator() {
	connStr, err := buildConnectionString(&MSSQLConf{Host: "sql.example.com", Port: 1433, Database: "warehouse", User: "recon", Password: "secret"})
	s.Require().NoError(err)

	u, err := url.Parse(connStr)
	s.Require().NoError(err)
	s.Equal("recon", u.User.Username())
	s.False(u.Query().Has(paramAuthenticator))
}

func (s *IntegratedAuthSuite) TestExecutorRejectsIntegratedAuthWithCredentials() {
	for _, conf := range []MSSQLConf{
		{User: "DOMAIN\\me"},
		{Password: "secret"},
		{AccessToken: "token"},
		{FedAuth: "ActiveDirectoryDefault"},
	} {
		conf.Host = "sql.example.com"
		conf.Database = "warehouse"
		conf.IntegratedAuth = true
		_, err := NewMSSQLExecutor(context.Background(), &conf)
		s.Require().ErrorIs(err, errIntegratedAuthWithCredentials)
	}
}

func env(values map[string]string) func(string) string {
	return func(key string) string { return values[key] }
}

func existing(paths ...string) func(string) bool {
	return func(path string) bool {
		for _, p := range paths {
			if p == path {
				return true
			}
		}
		return false
	}
}
