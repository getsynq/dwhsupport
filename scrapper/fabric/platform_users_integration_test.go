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

// TestFabricPlatformUsers_WorkspaceRoleDecidesTheSource checks both sources
// against what the test service principal may do. The database source always
// answers. With the Member or Admin workspace role the API source lists the
// role assignments; without it (the dwhtesting principal is a Contributor) it
// is a refused source and the reconciled listing is limited.
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

	result, err := sc.QueryPlatformUsers(ctx)
	require.NoError(t, err)
	require.Len(t, result.Sources, 2, "both sources are always read")
	api, db := result.Sources[0], result.Sources[1]
	assert.Equal(t, sourceWorkspaceRoleAssignments, api.Source)
	assert.Equal(t, sourceDatabasePrincipals, db.Source)
	require.Empty(t, db.Refused, "the test principal may read its database")

	me := fabricConnectedLogin(t)
	find := func(src *scrapper.PlatformUserListing) *scrapper.PlatformUser {
		for _, u := range src.Users {
			if strings.EqualFold(u.Login, me) {
				return u
			}
		}
		return nil
	}
	mine := find(db)
	require.NotNil(t, mine, "the database source names the connected principal by the login query history reports")
	assert.Equal(t, "EXTERNAL_USER", mine.Type)
	assert.Equal(t, scrapper.PlatformUsersLimited, db.Completeness)

	if apiErr == nil {
		require.Empty(t, api.Refused)
		fromAPI := find(api)
		require.NotNil(t, fromAPI)
		assert.Equal(t, "ServicePrincipal", fromAPI.Type)
		assert.NotEmpty(t, fromAPI.Roles, "a workspace role")
		return
	}
	require.True(t, sc.IsPermissionError(apiErr), "the API refused rather than failed: %v", apiErr)
	assert.Contains(t, api.Refused, "InsufficientWorkspaceRole")
	assert.Empty(t, api.Users)
	reconciled := result.Reconcile()
	assert.Equal(t, scrapper.PlatformUsersLimited, reconciled.Completeness)
	assert.Contains(t, reconciled.CompletenessReason, sourceWorkspaceRoleAssignments+" refused")
}
