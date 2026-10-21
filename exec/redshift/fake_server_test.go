package redshift

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"io"
	"math/big"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// fakeServer speaks just enough of the Postgres wire protocol for lib/pq to
// connect over TLS (its default sslmode=require does not verify the
// certificate), ping, and run a query with parameters through the extended
// protocol. Every query returns one text column n holding "1".
//
// The first duplicateBindCompletes Executes the server sees are answered with
// BindComplete twice. Redshift Serverless does that to the first
// query that needs compute after the workgroup was idle, and lib/pq then fails
// the query with "unexpected message during extended query execution: '2'".
type fakeServer struct {
	t                      *testing.T
	listener               net.Listener
	tlsConfig              *tls.Config
	duplicateBindCompletes int32

	connections atomic.Int32
	executes    atomic.Int32
	wg          sync.WaitGroup
}

func newFakeServer(t *testing.T, duplicateBindCompletes int32) *fakeServer {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	s := &fakeServer{t: t, listener: listener, tlsConfig: selfSignedTLS(t), duplicateBindCompletes: duplicateBindCompletes}
	s.wg.Add(1)
	go s.accept()
	t.Cleanup(func() {
		_ = listener.Close()
		s.wg.Wait()
	})
	return s
}

func (s *fakeServer) conf() *RedshiftConf {
	addr := s.listener.Addr().(*net.TCPAddr)
	return &RedshiftConf{User: "u", Password: "p", Database: "dev", Host: "127.0.0.1", Port: addr.Port}
}

func (s *fakeServer) accept() {
	defer s.wg.Done()
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return
		}
		s.connections.Add(1)
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer conn.Close()
			_ = conn.SetDeadline(time.Now().Add(30 * time.Second))
			s.serve(conn)
		}()
	}
}

func (s *fakeServer) serve(raw net.Conn) {
	var conn net.Conn = raw
	r := bufio.NewReader(conn)
	// Startup: an SSLRequest first, then the startup message over TLS.
	code, ok := readStartup(r)
	if !ok {
		return
	}
	if code == 80877103 {
		if _, err := raw.Write([]byte{'S'}); err != nil {
			return
		}
		tlsConn := tls.Server(raw, s.tlsConfig)
		if err := tlsConn.Handshake(); err != nil {
			return
		}
		conn = tlsConn
		r = bufio.NewReader(conn)
		if _, ok := readStartup(r); !ok {
			return
		}
	}
	w := bufio.NewWriter(conn)
	writeMessage(w, 'R', int32Bytes(0)) // AuthenticationOk
	writeMessage(w, 'Z', []byte{'I'})
	if w.Flush() != nil {
		return
	}

	var parsed, bound bool
	for {
		t, ok := readMessage(r)
		if !ok {
			return
		}
		switch t {
		case 'Q': // simple query, the ping's ";"
			writeMessage(w, 'I', nil)
			writeMessage(w, 'Z', []byte{'I'})
		case 'P':
			parsed = true
		case 'B':
			bound = true
		case 'D', 'E', 'C', 'H':
		case 'S':
			if parsed {
				writeMessage(w, '1', nil)                                      // ParseComplete
				writeMessage(w, 't', append(int16Bytes(1), int32Bytes(25)...)) // one text parameter
				writeMessage(w, 'T', rowDescription())
				parsed = false
			}
			if bound {
				writeMessage(w, '2', nil) // BindComplete
				if s.executes.Add(1) <= s.duplicateBindCompletes {
					writeMessage(w, '2', nil)
				}
				writeMessage(w, 'D', append(int16Bytes(1), append(int32Bytes(1), '1')...))
				writeMessage(w, 'C', []byte("SELECT 1\x00"))
				bound = false
			}
			writeMessage(w, 'Z', []byte{'I'})
		case 'X':
			return
		}
		if w.Flush() != nil {
			return
		}
	}
}

func rowDescription() []byte {
	b := int16Bytes(1)
	b = append(b, "n\x00"...)
	b = append(b, int32Bytes(0)...)  // table oid
	b = append(b, int16Bytes(0)...)  // attribute number
	b = append(b, int32Bytes(25)...) // text, which lib/pq reads in text format
	b = append(b, int16Bytes(-1)...)
	b = append(b, int32Bytes(-1)...)
	b = append(b, int16Bytes(0)...) // text format
	return b
}

func readStartup(r *bufio.Reader) (int32, bool) {
	var header [8]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return 0, false
	}
	length := int32(binary.BigEndian.Uint32(header[:4]))
	if _, err := io.CopyN(io.Discard, r, int64(length-8)); err != nil {
		return 0, false
	}
	return int32(binary.BigEndian.Uint32(header[4:])), true
}

// readMessage reads one message and returns its type; the fake needs no body.
func readMessage(r *bufio.Reader) (byte, bool) {
	var header [5]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return 0, false
	}
	if _, err := io.CopyN(io.Discard, r, int64(binary.BigEndian.Uint32(header[1:]))-4); err != nil {
		return 0, false
	}
	return header[0], true
}

func writeMessage(w *bufio.Writer, t byte, body []byte) {
	_ = w.WriteByte(t)
	_, _ = w.Write(int32Bytes(int32(len(body) + 4)))
	_, _ = w.Write(body)
}

func int32Bytes(v int32) []byte {
	return binary.BigEndian.AppendUint32(nil, uint32(v))
}

func int16Bytes(v int16) []byte {
	return binary.BigEndian.AppendUint16(nil, uint16(v))
}

func selfSignedTLS(t *testing.T) *tls.Config {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)
	return &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}}
}

// connectToFake builds the executor the product builds, pointed at the fake.
func connectToFake(t *testing.T, s *fakeServer) *RedshiftExecutor {
	t.Helper()
	executor, err := NewRedshiftExecutor(context.Background(), s.conf())
	require.NoError(t, err)
	t.Cleanup(func() { _ = executor.Close() })
	return executor
}
