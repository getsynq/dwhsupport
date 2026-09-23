package db2

import (
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestIsPermissionError(t *testing.T) {
	cases := map[string]bool{
		// what Db2 12.1 answered a reader without EXECUTE on MON_GET_CONNECTION
		"db2: SQLCODE=-551 SQLSTATE=42501 SYNQ_READER, EXECUTE, SYSPROC.MON_GET_CONNECTION": true,
		"db2: SQLERRP:SQLRA143 SQLCODE:-552":                                                true,
		"db2: SQLCODE=-1060 SQLSTATE=08004 SYNQ_READER":                                     true,
		"db2: SQLCODE=-104 SQLSTATE=42601 SELEC, BEGIN-OF-STATEMENT":                        false,
		"db2: SQLCODE=-5510 SQLSTATE=42XXX":                                                 false,
		"authentication failed: SECCHKCD=15":                                                false,
	}
	for msg, want := range cases {
		assert.Equal(t, want, IsPermissionError(errors.New(msg)), msg)
	}
	assert.False(t, IsPermissionError(nil))
}

func TestDriverConfig(t *testing.T) {
	cfg, err := driverConfig(&Db2Conf{Hostname: "h", Database: "SAMPLE", User: "u", Password: "p"})
	assert.NoError(t, err)
	assert.Equal(t, 50000, cfg.Port)
	assert.False(t, cfg.UseSSL)
	assert.EqualValues(t, 3, cfg.SecurityMechanism)

	cfg, err = driverConfig(&Db2Conf{Hostname: "h", Security: "ssl", SSLServerCertificate: "/ca.pem", Authentication: "server_encrypt"})
	assert.NoError(t, err)
	assert.Equal(t, 50001, cfg.Port)
	assert.True(t, cfg.UseSSL)
	assert.Equal(t, "/ca.pem", cfg.SSLRootCAPath)
	assert.EqualValues(t, 9, cfg.SecurityMechanism)

	_, err = driverConfig(&Db2Conf{Security: "TLS"})
	assert.Error(t, err)
	_, err = driverConfig(&Db2Conf{Authentication: "KERBEROS"})
	assert.Error(t, err)
}
