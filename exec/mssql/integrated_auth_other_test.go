//go:build !windows

package mssql

import (
	"fmt"
	"net/url"
	"os"
	"path/filepath"
)

func (s *IntegratedAuthSuite) TestConnectionStringSelectsKerberos() {
	cache := filepath.Join(s.T().TempDir(), "krb5cc")
	s.Require().NoError(os.WriteFile(cache, []byte("ticket"), 0o600))
	s.T().Setenv("KRB5CCNAME", "FILE:"+cache)

	connStr, err := buildConnectionString(&MSSQLConf{Host: "sql.example.com", Port: 1433, Database: "warehouse", IntegratedAuth: true})
	s.Require().NoError(err)

	u, err := url.Parse(connStr)
	s.Require().NoError(err)
	s.Nil(u.User, "integrated auth must not send a SQL Server login")
	s.Equal(authenticatorKerberos, u.Query().Get(paramAuthenticator))
	s.Equal(cache, u.Query().Get(paramKrb5CredCacheFile))
	s.Equal("warehouse", u.Query().Get("database"))
}

func (s *IntegratedAuthSuite) TestConnectionStringFailsWithoutTicket() {
	s.T().Setenv("KRB5CCNAME", fmt.Sprintf("FILE:%s", filepath.Join(s.T().TempDir(), "missing")))

	_, err := buildConnectionString(&MSSQLConf{Host: "sql.example.com", Port: 1433, Database: "warehouse", IntegratedAuth: true})
	s.Require().Error(err)
	s.Contains(err.Error(), "does not exist")
}
