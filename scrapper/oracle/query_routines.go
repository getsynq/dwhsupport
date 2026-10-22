package oracle

import (
	"context"
	"regexp"
	"strings"

	dwhexecoracle "github.com/getsynq/dwhsupport/exec/oracle"
	"github.com/getsynq/dwhsupport/logging"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scope"
)

// routineOwnerExclusions are the Oracle-maintained schemas. Oracle installs
// routines of its own in schemas no list here keeps up with (Database Vault's
// DVF), so every user ALL_USERS marks ORACLE_MAINTAINED is left out too.
const routineOwnerExclusions = `(
        'SYS', 'SYSTEM', 'OUTLN', 'DBSNMP', 'APPQOSSYS', 'DBSFWUSER',
        'GGSYS', 'GSMADMIN_INTERNAL', 'XDB', 'WMSYS', 'OJVMSYS',
        'CTXSYS', 'ORDSYS', 'ORDDATA', 'MDSYS', 'LBACSYS',
        'DVSYS', 'AUDSYS', 'OLAPSYS', 'REMOTE_SCHEDULER_AGENT',
        'ANONYMOUS', 'XS$NULL', 'GGSHAREDCAP', 'MDDATA',
        'DVF', 'SPATIAL_CSW_ADMIN_USR', 'SPATIAL_WFS_ADMIN_USR',
        'SYSBACKUP', 'SYSDG', 'SYSKM', 'SYSRAC', 'SYS$UMF',
        'DIP', 'ORACLE_OCM', 'FLOWS_FILES',
        'VECSYS', 'RASADM', 'GSMCATUSER', 'GSMUSER'
    )`

// oracleMaintainedOwners is every user Oracle maintains.
const oracleMaintainedOwners = `(SELECT USERNAME FROM ALL_USERS WHERE ORACLE_MAINTAINED = 'Y')`

// Standalone procedures and functions. A procedure or function declared in a
// package is part of the package and has no object of its own.
const queryRoutinesSql = `
SELECT
    o.OWNER        AS "schema",
    o.OBJECT_NAME  AS "name",
    o.OBJECT_TYPE  AS "kind",
    o.OBJECT_TYPE  AS "routine_type"
FROM
    {VIEW_PREFIX}_OBJECTS o
WHERE
    o.OBJECT_TYPE IN ('PROCEDURE', 'FUNCTION')
    AND o.OWNER NOT IN ` + routineOwnerExclusions + `
    AND o.OWNER NOT IN ` + oracleMaintainedOwners + `
    /* SYNQ_SCOPE_FILTER */
ORDER BY
    o.OWNER, o.OBJECT_NAME`

// The source of a stored routine is kept one row per line, from its PROCEDURE
// or FUNCTION keyword on, each line with its own line break.
const queryRoutineSourceSql = `
SELECT
    s.OWNER  AS "schema",
    s.NAME   AS "name",
    s.TYPE   AS "kind",
    s.TEXT   AS "text"
FROM
    {VIEW_PREFIX}_SOURCE s
WHERE
    s.TYPE IN ('PROCEDURE', 'FUNCTION')
    AND s.OWNER NOT IN ` + routineOwnerExclusions + `
    AND s.OWNER NOT IN ` + oracleMaintainedOwners + `
    /* SYNQ_SCOPE_FILTER */
ORDER BY
    s.OWNER, s.NAME, s.TYPE, s.LINE`

type routineSourceLine struct {
	Schema string `db:"schema"`
	Name   string `db:"name"`
	Kind   string `db:"kind"`
	Text   string `db:"text"`
}

// wrappedSource matches the first line of a routine created from wrapped
// (obfuscated) source, whose text cannot be read back as SQL.
var wrappedSource = regexp.MustCompile(`(?i)^\s*(procedure|function)\s+\S+\s+wrapped\b`)

// QueryRoutines lists the standalone procedures and functions with their
// source, from DBA_OBJECTS and DBA_SOURCE, which SELECT ANY DICTIONARY or
// SELECT_CATALOG_ROLE lets a role read. ALL_SOURCE lists a routine only to a
// role that may execute or debug it, so it is read only when the DBA views were
// refused.
func (e *OracleScrapper) QueryRoutines(ctx context.Context) ([]*scrapper.RoutineRow, error) {
	rows, err := e.queryRoutines(ctx, "DBA")
	if isRefused(err) {
		logging.GetLogger(ctx).WithError(err).Warn("cannot read DBA_OBJECTS and DBA_SOURCE, listing routines from ALL_OBJECTS and ALL_SOURCE")
		return e.queryRoutines(ctx, "ALL")
	}
	return rows, err
}

func (e *OracleScrapper) queryRoutines(ctx context.Context, viewPrefix string) ([]*scrapper.RoutineRow, error) {
	routinesSql := strings.ReplaceAll(queryRoutinesSql, "{VIEW_PREFIX}", viewPrefix)
	routines, err := dwhexecoracle.NewQuerier[scrapper.RoutineRow](e.executor).QueryMany(ctx,
		scope.AppendScopeConditions(ctx, routinesSql, "", "o.OWNER", "o.OBJECT_NAME"),
	)
	if err != nil {
		return nil, err
	}

	sourceSql := strings.ReplaceAll(queryRoutineSourceSql, "{VIEW_PREFIX}", viewPrefix)
	lines, err := dwhexecoracle.NewQuerier[routineSourceLine](e.executor).QueryMany(ctx,
		scope.AppendScopeConditions(ctx, sourceSql, "", "s.OWNER", "s.NAME"),
	)
	if err != nil {
		return nil, err
	}

	for _, routine := range routines {
		routine.Instance = e.conf.Host
		routine.Database = e.conf.ServiceName
	}
	attachRoutineSource(routines, lines)
	return routines, nil
}

// attachRoutineSource sets each routine's definition to its source lines,
// joined and preceded by the CREATE OR REPLACE the source leaves out. A routine
// whose source is wrapped, or that has no lines, keeps no definition.
func attachRoutineSource(routines []*scrapper.RoutineRow, lines []*routineSourceLine) {
	type routineKey struct{ schema, name, kind string }
	sources := map[routineKey]*strings.Builder{}
	for _, line := range lines {
		key := routineKey{line.Schema, line.Name, line.Kind}
		source, ok := sources[key]
		if !ok {
			source = &strings.Builder{}
			sources[key] = source
		}
		source.WriteString(line.Text)
	}

	for _, routine := range routines {
		source, ok := sources[routineKey{routine.Schema, routine.Name, string(routine.Kind)}]
		if !ok {
			continue
		}
		text := strings.TrimSpace(source.String())
		if text == "" || wrappedSource.MatchString(text) {
			continue
		}
		definition := "CREATE OR REPLACE " + text
		routine.Definition = &definition
	}
}
