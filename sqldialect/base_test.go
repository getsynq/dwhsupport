package sqldialect

import (
	"testing"

	"github.com/gkampitakis/go-snaps/snaps"
	"github.com/stretchr/testify/suite"
)

type BaseSuite struct {
	suite.Suite
}

func TestBaseSuite(t *testing.T) {
	suite.Run(t, new(BaseSuite))
}

func (s *BaseSuite) TestCte() {
	dialect := NewPostgresDialect()
	cteSelect := NewSelect().Cols(Star()).
		From(TableFqn("proj", "default", "users")).
		OrderBy(Desc(Identifier("created_at"))).
		WithLimit(Limit(Int64(10)))
	cteFqn := CteFqn("cte")

	cte := NewSelect().Cte(cteFqn, cteSelect).From(cteFqn).Cols(Star())
	sql, err := cte.ToSql(dialect)
	s.Require().NoError(err)
	snaps.MatchSnapshot(s.T(), sql)
}
