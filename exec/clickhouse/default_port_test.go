package clickhouse

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A connection without a port must dial the native port that matches its
// transport: 9440 over TLS, 9000 in plain text. ClickHouse Cloud answers TLS
// only on 9440, so dialing 9000 with TLS on fails.
func TestNewClickhouseExecutorDefaultPort(t *testing.T) {
	cases := []struct {
		name  string
		noSsl bool
		port  int
		want  int
	}{
		{name: "tls", noSsl: false, want: 9440},
		{name: "plain text", noSsl: true, want: 9000},
		{name: "tls with an explicit port", noSsl: false, port: 9000, want: 9000},
		{name: "plain text with an explicit port", noSsl: true, port: 9440, want: 9440},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			// The .invalid domain never resolves, so the ping fails without dialing.
			conf := &ClickhouseConf{Hostname: "clickhouse.invalid", Port: tc.port, NoSsl: tc.noSsl}
			_, err := NewClickhouseExecutor(ctx, conf)
			require.Error(t, err)
			require.Equal(t, tc.want, conf.Port)
		})
	}
}
