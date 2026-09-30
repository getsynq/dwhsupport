package databricks

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/suite"
)

type ServicePrincipalNamesSuite struct {
	suite.Suite
	calls []string
}

func TestServicePrincipalNamesSuite(t *testing.T) {
	suite.Run(t, new(ServicePrincipalNamesSuite))
}

func (s *ServicePrincipalNamesSuite) SetupTest() {
	s.calls = nil
}

func (s *ServicePrincipalNamesSuite) lookup(names map[string]string, err error) servicePrincipalLookup {
	return func(_ context.Context, applicationId string) (string, error) {
		s.calls = append(s.calls, applicationId)
		if err != nil {
			return "", err
		}
		return names[applicationId], nil
	}
}

const (
	etlApplicationId   = "8b2d6a3e-1f4c-4b7a-9c0e-2d5f6a7b8c9d"
	otherApplicationId = "1c9e2f40-6a1b-4d3e-8f2a-7b6c5d4e3f21"
)

func (s *ServicePrincipalNamesSuite) TestResolvesApplicationIdOncePerFetch() {
	names := newServicePrincipalNames(s.lookup(map[string]string{etlApplicationId: "etl-runner"}, nil))

	s.Equal("etl-runner", names.DisplayName(context.Background(), etlApplicationId))
	s.Equal("etl-runner", names.DisplayName(context.Background(), etlApplicationId))
	s.Equal([]string{etlApplicationId}, s.calls)
}

func (s *ServicePrincipalNamesSuite) TestUnknownApplicationIdIsRememberedAsUnnamed() {
	names := newServicePrincipalNames(s.lookup(map[string]string{}, nil))

	s.Empty(names.DisplayName(context.Background(), otherApplicationId))
	s.Empty(names.DisplayName(context.Background(), otherApplicationId))
	s.Equal([]string{otherApplicationId}, s.calls)
}

func (s *ServicePrincipalNamesSuite) TestHumanLoginsAreNeverLookedUp() {
	names := newServicePrincipalNames(s.lookup(map[string]string{}, nil))

	for _, userName := range []string{"", "analyst@example.com", "not-a-uuid"} {
		s.Empty(names.DisplayName(context.Background(), userName))
	}
	s.Empty(s.calls)
}

func (s *ServicePrincipalNamesSuite) TestFailedLookupStopsResolutionForTheFetch() {
	names := newServicePrincipalNames(s.lookup(nil, errors.New("PERMISSION_DENIED")))

	s.Empty(names.DisplayName(context.Background(), etlApplicationId))
	s.Empty(names.DisplayName(context.Background(), otherApplicationId))
	s.Empty(names.DisplayName(context.Background(), etlApplicationId))
	s.Equal([]string{etlApplicationId}, s.calls)
}

func (s *ServicePrincipalNamesSuite) TestNamesResolvedBeforeAFailureAreKept() {
	failing := false
	names := newServicePrincipalNames(func(_ context.Context, applicationId string) (string, error) {
		if failing {
			return "", errors.New("rate limited")
		}
		return "etl-runner", nil
	})

	s.Equal("etl-runner", names.DisplayName(context.Background(), etlApplicationId))
	failing = true
	s.Empty(names.DisplayName(context.Background(), otherApplicationId))
	s.Equal("etl-runner", names.DisplayName(context.Background(), etlApplicationId))
}
