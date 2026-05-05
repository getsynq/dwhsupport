package athena

import (
	"context"
	"strings"
	"testing"
)

// TestExecutor_RefusesDefaultChainByDefault is the security gate: an AthenaConf
// with no explicit auth fields must NOT silently fall through to the AWS
// default credential chain (which would attach to the host process's identity
// — a real risk on the SYNQ cloud backend).
func TestExecutor_RefusesDefaultChainByDefault(t *testing.T) {
	_, err := NewAthenaExecutor(context.Background(), &AthenaConf{
		Region: "eu-central-1",
		// No AccessKeyID / AwsProfile / RoleArn / AllowDefaultChain.
	})
	if err == nil {
		t.Fatal("expected error refusing default credential chain, got nil")
	}
	if !strings.Contains(err.Error(), "no authentication method configured") {
		t.Fatalf("expected 'no authentication method configured' error, got: %v", err)
	}
}

// TestExecutor_AllowDefaultChainPath confirms the opt-in flag DOES advance
// past the gate. The build will then fail downstream (no real creds available
// in the test environment); we only assert the gate itself permits it.
func TestExecutor_AllowDefaultChainPath(t *testing.T) {
	_, err := NewAthenaExecutor(context.Background(), &AthenaConf{
		Region:            "eu-central-1",
		AllowDefaultChain: true,
	})
	// We expect a downstream error (sts:GetCallerIdentity / no creds) — NOT
	// the gate error. Anything other than the gate message proves we got past it.
	if err != nil && strings.Contains(err.Error(), "no authentication method configured") {
		t.Fatalf("AllowDefaultChain=true should bypass the gate, got: %v", err)
	}
}

// TestExecutor_PartialStaticCredsRejected ensures we don't accidentally accept
// a dangling AccessKeyID without a SecretAccessKey (would otherwise fall through).
func TestExecutor_PartialStaticCredsRejected(t *testing.T) {
	_, err := NewAthenaExecutor(context.Background(), &AthenaConf{
		Region:      "eu-central-1",
		AccessKeyID: "AKIASOMETHING",
		// SecretAccessKey deliberately empty.
	})
	if err == nil {
		t.Fatal("expected error rejecting partial static creds, got nil")
	}
	if !strings.Contains(err.Error(), "must be provided together") {
		t.Fatalf("expected 'must be provided together' error, got: %v", err)
	}
}
