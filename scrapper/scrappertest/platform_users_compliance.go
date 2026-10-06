package scrappertest

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/getsynq/dwhsupport/exec/querycontext"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/stretchr/testify/suite"
)

// PlatformUsersSuite checks QueryPlatformUsers against a real warehouse. Embed
// it in a warehouse's integration suite, set Scrapper, and set ConnectedLogin
// to the login the scrapper connects as: it is the one user every listing must
// contain, whatever else the role may see.
//
// A platform without a user listing must say so through its capability and
// return ErrUnsupported; one with a listing must return it, never an empty
// "complete" one.
type PlatformUsersSuite struct {
	suite.Suite
	Scrapper scrapper.Scrapper
	// ConnectedLogin is the login the connection authenticates as, spelled as
	// the platform lists it.
	ConnectedLogin string
	// MatchLoginFold compares ConnectedLogin case-insensitively, for platforms
	// that fold an unquoted login (Snowflake upper-cases, Postgres lower-cases)
	// so the configured spelling need not match the listed one.
	MatchLoginFold bool
	// ExpectFacts are facts this warehouse's test login is known to have, so a
	// regression that silently stops reading one fails here.
	ExpectFacts []scrapper.PlatformUserFact
}

func (s *PlatformUsersSuite) TestPlatformUsers_CapabilityMatchesCall() {
	if s.Scrapper == nil {
		s.T().Skip("Scrapper not set")
	}
	capability := s.Scrapper.Capabilities().PlatformUsers
	users, err := s.Scrapper.QueryPlatformUsers(s.ctx())
	if !capability.Supported {
		s.Require().ErrorIs(err, scrapper.ErrUnsupported, "an unsupported capability must return ErrUnsupported")
		s.Nil(users)
		return
	}
	s.NotEmpty(capability.Grant, "a supported capability names the grant that lets a role see every user")
	if s.Scrapper.IsPermissionError(err) {
		s.T().Skipf("the test role may not list users: %v", err)
	}
	s.Require().NoError(err)
	s.Require().NotNil(users)
}

func (s *PlatformUsersSuite) TestPlatformUsers_ListsTheConnectedLogin() {
	users := s.listing().Reconcile()

	s.NotEqual(scrapper.PlatformUsersEmpty, users.Completeness, "the role we connect as is a user, so the listing cannot be empty")
	s.NotEmpty(users.Completeness)
	if users.Completeness != scrapper.PlatformUsersComplete {
		s.NotEmpty(users.CompletenessReason, "a listing that is not complete says why")
	}

	s.Require().NotEmpty(s.ConnectedLogin, "set ConnectedLogin")
	me := s.find(users, s.ConnectedLogin)
	s.Require().NotNilf(me, "the connected login %q is not listed among %d users", s.ConnectedLogin, len(users.Users))

	for _, fact := range s.ExpectFacts {
		s.Falsef(users.IsSkipped(fact), "fact %s was skipped: %v", fact, users.SkippedFacts)
		s.Truef(hasFact(me, fact), "the connected login has no %s", fact)
	}
}

func (s *PlatformUsersSuite) TestPlatformUsers_Sources() {
	result := s.listing()

	s.Require().NotEmpty(result.Sources)
	s.False(result.AllRefused(), "a result whose every source refused is a permission error, not a result")
	names := map[string]bool{}
	for _, src := range result.Sources {
		s.NotEmpty(src.Source)
		s.NotEqual(scrapper.PlatformUsersReconciledSource, src.Source)
		s.Falsef(names[src.Source], "source %q listed twice", src.Source)
		names[src.Source] = true
		s.Containsf([]scrapper.PlatformUserSourceKind{scrapper.PlatformUserSourceSQL, scrapper.PlatformUserSourceAPI}, src.Kind,
			"source %q has kind %q", src.Source, src.Kind)
		if src.Refused != "" {
			s.Emptyf(src.Users, "refused source %q lists users", src.Source)
			continue
		}
		s.checkShape(src)
	}
}

func (s *PlatformUsersSuite) TestPlatformUsers_Shape() {
	s.checkShape(s.listing().Reconcile())
}

func (s *PlatformUsersSuite) checkShape(users *scrapper.PlatformUserListing) {
	s.NotEmptyf(users.Completeness, "%s has no completeness", users.Source)
	s.True(slices.IsSortedFunc(users.Users, func(a, b *scrapper.PlatformUser) int { return strings.Compare(a.Login, b.Login) }),
		"users are sorted by login")
	seen := map[string]bool{}
	for _, u := range users.Users {
		s.NotEmpty(u.Login)
		s.Falsef(seen[u.Login], "login %q listed twice", u.Login)
		seen[u.Login] = true
		s.Truef(slices.IsSorted(u.Roles), "roles of %q are not sorted: %v", u.Login, u.Roles)
		s.Equalf(len(slices.Compact(slices.Clone(u.Roles))), len(u.Roles), "roles of %q have duplicates: %v", u.Login, u.Roles)
		if u.CreatedAt != nil {
			s.Equalf("UTC", u.CreatedAt.Location().String(), "created_at of %q is not UTC", u.Login)
		}
		if u.LastLoginAt != nil {
			s.Equalf("UTC", u.LastLoginAt.Location().String(), "last_login_at of %q is not UTC", u.Login)
		}
	}
	for _, f := range users.SkippedFacts {
		s.NotEmptyf(f.Reason, "skipped fact %s has no reason", f.Fact)
	}
}

func (s *PlatformUsersSuite) listing() *scrapper.PlatformUsers {
	if s.Scrapper == nil {
		s.T().Skip("Scrapper not set")
	}
	if !s.Scrapper.Capabilities().PlatformUsers.Supported {
		s.T().Skip("platform has no user listing")
	}
	users, err := s.Scrapper.QueryPlatformUsers(s.ctx())
	if s.Scrapper.IsPermissionError(err) {
		s.T().Skipf("the test role may not list users: %v", err)
	}
	s.Require().NoError(err)
	s.Require().NotNil(users)
	return users
}

func (s *PlatformUsersSuite) ctx() context.Context {
	return querycontext.WithQueryContext(context.Background(), complianceQueryContext)
}

func (s *PlatformUsersSuite) find(users *scrapper.PlatformUserListing, login string) *scrapper.PlatformUser {
	for _, u := range users.Users {
		if u.Login == login || (s.MatchLoginFold && strings.EqualFold(u.Login, login)) {
			return u
		}
	}
	return nil
}

func hasFact(u *scrapper.PlatformUser, fact scrapper.PlatformUserFact) bool {
	switch fact {
	case scrapper.PlatformUserFactPlatformId:
		return u.PlatformId != ""
	case scrapper.PlatformUserFactType:
		return u.Type != ""
	case scrapper.PlatformUserFactEmail:
		return u.Email != ""
	case scrapper.PlatformUserFactDisplayName:
		return u.DisplayName != ""
	case scrapper.PlatformUserFactComment:
		return u.Comment != ""
	case scrapper.PlatformUserFactDisabled:
		return u.Disabled != nil
	case scrapper.PlatformUserFactCreatedAt:
		return u.CreatedAt != nil
	case scrapper.PlatformUserFactLastLoginAt:
		return u.LastLoginAt != nil
	case scrapper.PlatformUserFactDefaultRole:
		return u.DefaultRole != ""
	case scrapper.PlatformUserFactRoles:
		return len(u.Roles) > 0
	}
	return false
}

// OnlyPlatformUserSource unwraps the result of a platform that reads a single
// source of users, for tests that assert on that listing. It passes an error
// through, and fails when the result holds any other number of sources.
func OnlyPlatformUserSource(result *scrapper.PlatformUsers, err error) (*scrapper.PlatformUserListing, error) {
	if err != nil || result == nil {
		return nil, err
	}
	if len(result.Sources) != 1 {
		return nil, fmt.Errorf("expected one source of users, got %d", len(result.Sources))
	}
	return result.Sources[0], nil
}
