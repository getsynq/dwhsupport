package sshtunnel

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/crypto/ssh"
)

type SshTunnel struct {
	Host       string
	Port       int
	User       string
	PrivateKey []byte
}

func (r *SshTunnel) IsEnabled() bool {
	return r != nil && r.Host != "" && len(r.PrivateKey) > 0
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
	return d.client.Dial(network, address)
}

func NewSshTunnelDialer(tunnel *SshTunnel) (*SshTunnelDialer, error) {
	if !tunnel.IsEnabled() {
		return nil, errors.New("tunnel is not enabled")
	}

	privateKey, err := ssh.ParsePrivateKey(tunnel.PrivateKey)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse private key")
	}

	sshConfig := &ssh.ClientConfig{
		User:            tunnel.User,
		Auth:            []ssh.AuthMethod{},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}
	sshConfig.Auth = append(sshConfig.Auth, ssh.PublicKeys(privateKey))

	client, err := ssh.Dial("tcp", fmt.Sprintf("%s:%d", tunnel.Host, tunnel.Port), sshConfig)
	if err != nil {
		return nil, errors.Wrap(err, "failed to dial ssh")
	}
	return &SshTunnelDialer{client: client}, nil
}
