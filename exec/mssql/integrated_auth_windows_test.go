//go:build windows

package mssql

import "net/url"

func (s *IntegratedAuthSuite) TestConnectionStringSelectsSSPI() {
	connStr, err := buildConnectionString(&MSSQLConf{Host: "sql.example.com", Port: 1433, Database: "warehouse", IntegratedAuth: true})
	s.Require().NoError(err)

	u, err := url.Parse(connStr)
	s.Require().NoError(err)
	s.Nil(u.User, "integrated auth must not send a SQL Server login")
	s.Equal(authenticatorWinSSPI, u.Query().Get(paramAuthenticator))
	s.False(u.Query().Has(paramKrb5CredCacheFile))
}
