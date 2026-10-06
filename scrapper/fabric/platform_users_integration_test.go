package fabric

import (
	"context"
	"os"
	"strings"
	"testing"

	dwhexecfabric "github.com/getsynq/dwhsupport/exec/fabric"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
	"github.com/getsynq/dwhsupport/testenv"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

// fabricConnectedLogin is the name Fabric gives the test service principal:
// <client id>@<tenant id>, the tenant read from the host when not configured.
func fabricConnectedLogin(t *testing.T) string {
	tenant := testenv.EnvOrDefault("FABRIC_TENANT_ID", "")
	if tenant == "" {
		identity, err := dwhexecfabric.ParseHostIdentity(testenv.EnvOrDefault("FABRIC_HOST", ""))
		require.NoError(t, err)
		tenant = identity.TenantID
	}
	return strings.ToLower(testenv.EnvOrDefault("FABRIC_CLIENT_ID", "") + "@" + tenant)
}

type FabricPlatformUsersSuite struct {
	scrappertest.PlatformUsersSuite
}

func TestFabricPlatformUsersSuite(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping Fabric platform users tests in CI")
	}
	suite.Run(t, new(FabricPlatformUsersSuite))
}

func (s *FabricPlatformUsersSuite) SetupSuite() {
	if testenv.EnvOrDefault("FABRIC_HOST", "") == "" || testenv.EnvOrDefault("FABRIC_CLIENT_ID", "") == "" {
		s.T().Skip("FABRIC_HOST / FABRIC_CLIENT_ID not set")
	}
	sc, err := newFabricScrapperFromEnv(context.Background())
	if err != nil {
		s.T().Skipf("Could not connect to Fabric: %v", err)
	}
	s.Scrapper = sc
	s.ConnectedLogin = fabricConnectedLogin(s.T())
	s.MatchLoginFold = true
	s.ExpectFacts = []scrapper.PlatformUserFact{scrapper.PlatformUserFactType}
}

func (s *FabricPlatformUsersSuite) TearDownSuite() {
	if s.Scrapper != nil {
		_ = s.Scrapper.Close()
	}
}

// TestFabricPlatformUsers_WorkspaceRoleDecidesTheSource checks the listing
// against what the test service principal may do. With the Member or Admin
// workspace role the role assignments are listed in full; without it (the
// dwhtesting principal is a Contributor) the API refuses and the listing falls
// back to the database's users, limited, naming the role that is missing.
func TestFabricPlatformUsers_WorkspaceRoleDecidesTheSource(t *testing.T) {
	if os.Getenv("CI") != "" || testenv.EnvOrDefault("FABRIC_HOST", "") == "" || testenv.EnvOrDefault("FABRIC_CLIENT_ID", "") == "" {
		t.Skip("FABRIC_HOST / FABRIC_CLIENT_ID not set")
	}
	ctx := context.Background()
	sc, err := newFabricScrapperFromEnv(ctx)
	if err != nil {
		t.Skipf("Could not connect to Fabric: %v", err)
	}
	defer sc.Close()

	identity, err := dwhexecfabric.ParseHostIdentity(sc.conf.Host)
	require.NoError(t, err)
	_, apiErr := sc.listRoleAssignments(ctx, identity.WorkspaceID)

	users, err := sc.QueryPlatformUsers(ctx)
	require.NoError(t, err)
	me := fabricConnectedLogin(t)
	var mine *scrapper.PlatformUser
	for _, u := range users.Users {
		if strings.EqualFold(u.Login, me) {
			mine = u
		}
	}
	require.NotNil(t, mine, "the connected service principal is listed under the login query history reports")

	if apiErr == nil {
		assert.Equal(t, "ServicePrincipal", mine.Type)
		assert.NotEmpty(t, mine.Roles, "a workspace role")
		return
	}
	require.True(t, sc.IsPermissionError(apiErr), "the API refused rather than failed: %v", apiErr)
	assert.Equal(t, scrapper.PlatformUsersLimited, users.Completeness)
	assert.Contains(t, users.CompletenessReason, "InsufficientWorkspaceRole")
	assert.Contains(t, users.CompletenessReason, "Member or Admin")
	assert.Equal(t, "EXTERNAL_USER", mine.Type)
}
