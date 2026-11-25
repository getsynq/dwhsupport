package db2

import (
	"context"
	"net"
	"testing"

	godb2 "github.com/go-db2/go-db2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// SERVER authentication over plain TCP/IP sends the password in the clear, as
// the IBM clients do. go-db2 refuses it unless the config allows it, so the
// executor must allow it, or every connection without SSL fails before it
// reaches the server.
func TestServerAuthenticationWithoutSSLReachesTheServer(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := l.Addr().(*net.TCPAddr).Port
	require.NoError(t, l.Close())

	cfg, err := driverConfig(&Db2Conf{Hostname: "127.0.0.1", Port: port, Database: "SAMPLE", User: "u", Password: "p"})
	require.NoError(t, err)

	_, err = godb2.NewConnector(cfg).Connect(context.Background())
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "plaintext authentication")
}
