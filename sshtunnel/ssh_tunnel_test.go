package sshtunnel

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/ssh"
)

func rsaPrivateKeyPem(t *testing.T) []byte {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	return pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
}

func ed25519PrivateKeyPem(t *testing.T) []byte {
	t.Helper()
	_, key, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	block, err := ssh.MarshalPrivateKey(key, "")
	require.NoError(t, err)
	return pem.EncodeToMemory(block)
}

func TestSshTunnelIsEnabled(t *testing.T) {
	key := rsaPrivateKeyPem(t)

	t.Run("nil tunnel", func(t *testing.T) {
		var tunnel *SshTunnel
		require.False(t, tunnel.IsEnabled())
	})

	t.Run("no host", func(t *testing.T) {
		require.False(t, (&SshTunnel{PrivateKeys: [][]byte{key}}).IsEnabled())
	})

	t.Run("no keys", func(t *testing.T) {
		require.False(t, (&SshTunnel{Host: "bastion"}).IsEnabled())
	})

	// A caller that builds the list positionally (current key, previous key) can
	// hand us an empty slot; that is not a usable key.
	t.Run("only empty key slots", func(t *testing.T) {
		require.False(t, (&SshTunnel{Host: "bastion", PrivateKeys: [][]byte{nil, {}}}).IsEnabled())
	})

	t.Run("one usable key after an empty slot", func(t *testing.T) {
		require.True(t, (&SshTunnel{Host: "bastion", PrivateKeys: [][]byte{nil, key}}).IsEnabled())
	})
}

func TestSshTunnelSigners(t *testing.T) {
	first := rsaPrivateKeyPem(t)
	second := ed25519PrivateKeyPem(t)

	t.Run("every key is offered, in the configured order", func(t *testing.T) {
		signers, err := (&SshTunnel{PrivateKeys: [][]byte{first, second}}).signers()
		require.NoError(t, err)
		require.Len(t, signers, 2)
		require.Equal(t, "ssh-rsa", signers[0].PublicKey().Type())
		require.Equal(t, "ssh-ed25519", signers[1].PublicKey().Type())
	})

	t.Run("empty slots are skipped", func(t *testing.T) {
		signers, err := (&SshTunnel{PrivateKeys: [][]byte{nil, first, {}}}).signers()
		require.NoError(t, err)
		require.Len(t, signers, 1)
	})

	// One unusable key must not take down a connection another key can still
	// authenticate — that is the whole point of offering more than one.
	t.Run("an unparseable key does not hide a usable one", func(t *testing.T) {
		signers, err := (&SshTunnel{PrivateKeys: [][]byte{[]byte("not a key"), first}}).signers()
		require.NoError(t, err)
		require.Len(t, signers, 1)
		require.Equal(t, "ssh-rsa", signers[0].PublicKey().Type())
	})

	t.Run("nothing usable reports every parse failure", func(t *testing.T) {
		_, err := (&SshTunnel{PrivateKeys: [][]byte{[]byte("nope"), []byte("also nope")}}).signers()
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to parse private key")
		require.Contains(t, err.Error(), "private key 0")
		require.Contains(t, err.Error(), "private key 1")
	})
}

func TestNewSshTunnelDialerRejectsDisabledTunnel(t *testing.T) {
	_, err := NewSshTunnelDialer(&SshTunnel{Host: "bastion"})
	require.ErrorContains(t, err, "tunnel is not enabled")
}
