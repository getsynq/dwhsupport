//go:build !windows

package mssql

import (
	"net/url"
	"os"

	// Registers the "krb5" integrated authentication provider; go-mssqldb only registers NTLM outside Windows.
	_ "github.com/microsoft/go-mssqldb/integratedauth/krb5"
)

// integratedAuthParams selects Kerberos with the ticket `kinit` left in the user's credential cache.
// The Kerberos configuration comes from KRB5_CONFIG or /etc/krb5.conf, read by the provider itself.
func integratedAuthParams() (url.Values, error) {
	cache, err := krb5CredCacheFile(os.Getenv, os.Getuid(), fileExists)
	if err != nil {
		return nil, err
	}
	return url.Values{
		paramAuthenticator:     {authenticatorKerberos},
		paramKrb5CredCacheFile: {cache},
	}, nil
}

func fileExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}
