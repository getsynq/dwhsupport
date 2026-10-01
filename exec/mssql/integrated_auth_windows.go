//go:build windows

package mssql

import "net/url"

// integratedAuthParams selects SSPI, which signs in as the logged-in Windows user. go-mssqldb
// registers the provider itself on Windows; naming it explicitly makes a failure to obtain the
// user's credentials an error rather than a silent fall back to SQL Server authentication.
func integratedAuthParams() (url.Values, error) {
	return url.Values{paramAuthenticator: {authenticatorWinSSPI}}, nil
}
