package db2

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

	cfg, err = driverConfig(&Db2Conf{Hostname: "h", Security: "ssl", SSLServerCertificateFile: "/ca.pem", Authentication: "server_encrypt"})
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

func TestDriverConfig_InlineCertificate(t *testing.T) {
	certPEM := selfSignedPEM(t)

	cfg, err := driverConfig(&Db2Conf{Hostname: "h", Security: "SSL", SSLServerCertificatePEM: certPEM})
	require.NoError(t, err)
	assert.True(t, cfg.UseSSL)
	assert.Empty(t, cfg.SSLRootCAPath)
	require.NotNil(t, cfg.TLSConfig)
	require.NotNil(t, cfg.TLSConfig.RootCAs)

	_, err = driverConfig(&Db2Conf{Hostname: "h", Security: "SSL", SSLServerCertificatePEM: "not a certificate"})
	assert.Error(t, err)

	_, err = driverConfig(&Db2Conf{Hostname: "h", Security: "SSL", SSLServerCertificatePEM: certPEM, SSLServerCertificateFile: "/ca.pem"})
	assert.Error(t, err)
}

// A certificate without Security SSL must not fall back to plain TCP/IP, which
// with the default SERVER authentication sends the password unencrypted.
func TestDriverConfig_CertificateWithoutSSL(t *testing.T) {
	_, err := driverConfig(&Db2Conf{Hostname: "h", SSLServerCertificatePEM: selfSignedPEM(t)})
	assert.Error(t, err)

	_, err = driverConfig(&Db2Conf{Hostname: "h", SSLServerCertificateFile: "/ca.pem"})
	assert.Error(t, err)
}

func selfSignedPEM(t *testing.T) string {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "db2 test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
}
