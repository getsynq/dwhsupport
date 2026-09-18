package sshtunnel

import (
	"context"
	stderrors "errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/crypto/ssh"
)

type SshTunnel struct {
	Host string
	Port int
	User string
	// PrivateKeys are the keys offered to the server, most preferred first. SSH
	// public-key auth tries them in order and the server accepts the first one
	// it trusts, so a caller rotating its key can offer the new key alongside
	// the one it replaces and no handshake breaks while the server's
	// authorized_keys is being updated.
	//
	// Offer only the keys that are still meant to work: OpenSSH counts every
	// attempt against MaxAuthTries (6 by default) and drops the connection once
	// it is exhausted, so a long list of retired keys can fail a handshake the
	// first entry would have completed.
	PrivateKeys [][]byte
	Timeout     time.Duration
}

func (r *SshTunnel) IsEnabled() bool {
	if r == nil || r.Host == "" {
		return false
	}
	for _, key := range r.PrivateKeys {
		if len(key) > 0 {
			return true
		}
	}
	return false
}

// signers parses every configured private key. Keys that fail to parse are
// skipped rather than failing the dial: with several keys offered, one unusable
// entry must not take down a connection another key can still authenticate. All
// parse errors are returned together when none of them parsed, so a tunnel with
// nothing usable still says why.
func (r *SshTunnel) signers() ([]ssh.Signer, error) {
	var signers []ssh.Signer
	var parseErrs []error
	for i, key := range r.PrivateKeys {
		if len(key) == 0 {
			continue
		}
		signer, err := ssh.ParsePrivateKey(key)
		if err != nil {
			parseErrs = append(parseErrs, errors.Wrapf(err, "private key %d", i))
			continue
		}
		signers = append(signers, signer)
	}
	if len(signers) == 0 {
		return nil, errors.Wrap(stderrors.Join(parseErrs...), "failed to parse private key")
	}
	return signers, nil
}

type SshTunnelDialer struct {
	client *ssh.Client
	mu     sync.Mutex
}

func (d *SshTunnelDialer) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.client == nil {
		return nil
	}
	err := d.client.Close()
	d.client = nil
	return err
}

func (d *SshTunnelDialer) Dial(network, address string) (net.Conn, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.client == nil {
		return nil, errors.New("dialer is closed")
	}
	return d.client.Dial(network, address)
}

func (d *SshTunnelDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.client == nil {
		return nil, errors.New("dialer is closed")
	}
	return d.client.DialContext(ctx, network, address)
}

func (d *SshTunnelDialer) DialTimeout(network, address string, timeout time.Duration) (net.Conn, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.client == nil {
		return nil, errors.New("dialer is closed")
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return d.client.DialContext(ctx, network, address)
}

func NewSshTunnelDialer(tunnel *SshTunnel) (*SshTunnelDialer, error) {
	if !tunnel.IsEnabled() {
		return nil, errors.New("tunnel is not enabled")
	}

	signers, err := tunnel.signers()
	if err != nil {
		return nil, err
	}

	timeout := tunnel.Timeout
	if timeout == 0 {
		timeout = 30 * time.Second
	}

	sshConfig := &ssh.ClientConfig{
		User:            tunnel.User,
		Auth:            []ssh.AuthMethod{ssh.PublicKeys(signers...)},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         timeout,
	}

	client, err := ssh.Dial("tcp", fmt.Sprintf("%s:%d", tunnel.Host, tunnel.Port), sshConfig)
	if err != nil {
		return nil, errors.Wrap(err, "failed to dial ssh")
	}
	return &SshTunnelDialer{client: client}, nil
}
