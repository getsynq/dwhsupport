package synq

import (
	"context"
	"crypto/tls"
	"net"
	"time"

	agentdwhv1 "github.com/getsynq/api/agent/dwh/v1"
	"golang.org/x/oauth2/clientcredentials"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/credentials/oauth"
	"google.golang.org/grpc/keepalive"
)

func NewGrpcConnection(ctx context.Context, config *agentdwhv1.Config, endpoint string) (*grpc.ClientConn, error) {
	host, _, err := net.SplitHostPort(endpoint)
	if err != nil {
		return nil, err
	}

	clientCredentialsConfig := &clientcredentials.Config{
		ClientID:     config.GetSynq().GetClientId(),
		ClientSecret: config.GetSynq().GetClientSecret(),
		TokenURL:     config.GetSynq().GetOauthUrl(),
	}
	oauthTokenSource := oauth.TokenSource{TokenSource: clientCredentialsConfig.TokenSource(ctx)}
	tlsCredentials := credentials.NewTLS(&tls.Config{})
	opts := []grpc.DialOption{
		grpc.WithAuthority(host),
	}
	if host == "localhost" {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	} else {
		opts = append(opts, grpc.WithPerRPCCredentials(oauthTokenSource))
		opts = append(opts, grpc.WithTransportCredentials(tlsCredentials))
	}

	var kacp = keepalive.ClientParameters{
		Time:                10 * time.Second, // send pings every 10 seconds if there is no activity
		Timeout:             time.Second,      // wait 1 second for ping ack before considering the connection dead
		PermitWithoutStream: true,             // send pings even without active streams
	}
	opts = append(opts, grpc.WithKeepaliveParams(kacp))

	return grpc.NewClient(endpoint, opts...)

}
