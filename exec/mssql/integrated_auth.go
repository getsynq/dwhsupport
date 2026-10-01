package mssql

import (
	"errors"
	"fmt"
	"strings"
)

// Connection-string parameters read by the go-mssqldb integrated authentication providers.
const (
	paramAuthenticator      = "authenticator"
	paramKrb5CredCacheFile  = "krb5-credcachefile"
	authenticatorWinSSPI    = "winsspi"
	authenticatorKerberos   = "krb5"
	krb5CredCacheEnv        = "KRB5CCNAME"
	krb5CredCacheFilePrefix = "FILE:"
)

var errIntegratedAuthWithCredentials = errors.New(
	"integrated authentication signs in as the current Windows or Kerberos identity: leave user, password, access token and fed auth empty",
)

// krb5CredCacheFile finds the Kerberos credential cache `kinit` wrote for this user. The Kerberos
// client go-mssqldb uses reads only file caches, so a KRB5CCNAME naming any other cache type
// (API:, KCM:, KEYRING:, DIR:) is rejected with the command that writes a file cache instead.
func krb5CredCacheFile(getenv func(string) string, uid int, exists func(string) bool) (string, error) {
	fileCacheHint := fmt.Sprintf("run `kinit -c FILE:/tmp/krb5cc_%d <user>@<REALM>` and set %s=/tmp/krb5cc_%d", uid, krb5CredCacheEnv, uid)

	cache := getenv(krb5CredCacheEnv)
	if cache == "" {
		cache = fmt.Sprintf("/tmp/krb5cc_%d", uid)
		if !exists(cache) {
			return "", fmt.Errorf("no Kerberos ticket found: %s is not set and %s does not exist, %s", krb5CredCacheEnv, cache, fileCacheHint)
		}
		return cache, nil
	}

	if strings.HasPrefix(cache, krb5CredCacheFilePrefix) {
		cache = strings.TrimPrefix(cache, krb5CredCacheFilePrefix)
	} else if prefix, _, found := strings.Cut(cache, ":"); found && !strings.Contains(prefix, "/") {
		return "", fmt.Errorf(
			"%s=%s is not a file credential cache, which is the only kind Kerberos sign-in can read: %s",
			krb5CredCacheEnv,
			cache,
			fileCacheHint,
		)
	}

	if !exists(cache) {
		return "", fmt.Errorf("the Kerberos credential cache %s (from %s) does not exist, %s", cache, krb5CredCacheEnv, fileCacheHint)
	}
	return cache, nil
}
