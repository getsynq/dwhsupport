package oracle

import (
	"testing"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAttachRoutineSource(t *testing.T) {
	routines := []*scrapper.RoutineRow{
		{Schema: "APP", Name: "REFRESH_TOTALS", Kind: scrapper.RoutineKindProcedure},
		{Schema: "APP", Name: "NET_AMOUNT", Kind: scrapper.RoutineKindFunction},
		{Schema: "APP", Name: "HIDDEN", Kind: scrapper.RoutineKindProcedure},
		{Schema: "APP", Name: "NO_SOURCE", Kind: scrapper.RoutineKindFunction},
		{Schema: "OTHER", Name: "REFRESH_TOTALS", Kind: scrapper.RoutineKindProcedure},
	}
	lines := []*routineSourceLine{
		{Schema: "APP", Name: "HIDDEN", Kind: "PROCEDURE", Text: "PROCEDURE hidden wrapped \n"},
		{Schema: "APP", Name: "HIDDEN", Kind: "PROCEDURE", Text: "a000000\n"},
		{Schema: "APP", Name: "NET_AMOUNT", Kind: "FUNCTION", Text: "FUNCTION net_amount(p NUMBER) RETURN NUMBER IS\n"},
		{Schema: "APP", Name: "NET_AMOUNT", Kind: "FUNCTION", Text: "BEGIN RETURN p; END;"},
		{Schema: "APP", Name: "REFRESH_TOTALS", Kind: "PROCEDURE", Text: "PROCEDURE refresh_totals AS\n"},
		{Schema: "APP", Name: "REFRESH_TOTALS", Kind: "PROCEDURE", Text: "BEGIN\n  INSERT INTO totals SELECT * FROM orders;\n"},
		{Schema: "APP", Name: "REFRESH_TOTALS", Kind: "PROCEDURE", Text: "END;\n"},
		{Schema: "OTHER", Name: "REFRESH_TOTALS", Kind: "PROCEDURE", Text: "procedure refresh_totals as begin null; end;"},
		// A package body is not a standalone routine and must not attach to one.
		{Schema: "APP", Name: "NO_SOURCE", Kind: "PACKAGE BODY", Text: "PACKAGE BODY no_source AS END;"},
	}

	attachRoutineSource(routines, lines)

	require.NotNil(t, routines[0].Definition)
	assert.Equal(t, "CREATE OR REPLACE PROCEDURE refresh_totals AS\nBEGIN\n  INSERT INTO totals SELECT * FROM orders;\nEND;", *routines[0].Definition)
	require.NotNil(t, routines[1].Definition)
	assert.Equal(t, "CREATE OR REPLACE FUNCTION net_amount(p NUMBER) RETURN NUMBER IS\nBEGIN RETURN p; END;", *routines[1].Definition)
	assert.Nil(t, routines[2].Definition, "wrapped source cannot be read as SQL")
	assert.Nil(t, routines[3].Definition, "a routine without source lines has no definition")
	require.NotNil(t, routines[4].Definition, "the same name in another schema is a routine of its own")
	assert.Equal(t, "CREATE OR REPLACE procedure refresh_totals as begin null; end;", *routines[4].Definition)
}
