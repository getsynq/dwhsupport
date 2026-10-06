package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"slices"
	"strings"

	dwhexecmysql "github.com/getsynq/dwhsupport/exec/mysql"
	"github.com/getsynq/dwhsupport/logging"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/go-sql-driver/mysql"
	"github.com/pkg/errors"
)

// platformUsersGrant is what lets a role list every account with its roles.
const platformUsersGrant = "SELECT on mysql.user, mysql.role_edges and mysql.default_roles " +
	"(MariaDB: mysql.global_priv and mysql.roles_mapping), for example GRANT SELECT ON mysql.* TO <user>"

// Server error numbers the listing tells apart from a permission error.
const (
	errUnknownColumn = 1054 // ER_BAD_FIELD_ERROR: a column this server version lacks
	errNoSuchTable   = 1146 // ER_NO_SUCH_TABLE: a table this server version lacks
)

// reservedAccounts are the accounts the server creates for its own use. They
// are locked and nobody signs in with them, so they are not listed.
var reservedAccounts = map[string]bool{
	"mysql.sys":        true,
	"mysql.session":    true,
	"mysql.infoschema": true,
	"mariadb.sys":      true,
}

// MySQL and MariaDB have no column for any of these.
var mysqlFactsNotOnPlatform = []scrapper.PlatformUserFact{
	scrapper.PlatformUserFactPlatformId,
	scrapper.PlatformUserFactType,
	scrapper.PlatformUserFactEmail,
	scrapper.PlatformUserFactDisplayName,
	scrapper.PlatformUserFactCreatedAt,
	scrapper.PlatformUserFactLastLoginAt,
}

// mysqlUsersSql reads the accounts. User_attributes (MySQL 8.0.21+) holds
// what CREATE USER ... COMMENT stored.
const mysqlUsersSql = `SELECT user, host, account_locked,
	JSON_UNQUOTE(JSON_EXTRACT(User_attributes, '$.metadata.comment')) AS comment
FROM mysql.user`

// mysqlUsersNoCommentSql is mysqlUsersSql for servers without User_attributes.
const mysqlUsersNoCommentSql = `SELECT user, host, account_locked FROM mysql.user`

// mysqlUserAttributesSql is the listing without SELECT on mysql.user: the view
// shows every account to a role that may read mysql.user or holds CREATE
// USER, and only the role's own account otherwise (MySQL 8.0.21+).
const mysqlUserAttributesSql = `SELECT user, host, attribute FROM information_schema.USER_ATTRIBUTES`

const mysqlRoleEdgesSql = `SELECT from_user, from_host, to_user, to_host FROM mysql.role_edges`

const mysqlDefaultRolesSql = `SELECT default_role_user AS from_user, default_role_host AS from_host, user AS to_user, host AS to_host
FROM mysql.default_roles`

// mariadbGlobalPrivSql reads MariaDB's accounts and roles (10.4+), whose
// lock, role flag and default role live in a JSON document.
const mariadbGlobalPrivSql = `SELECT user, host, priv FROM mysql.global_priv`

// mariadbUserSql is the listing on MariaDB before 10.4, which has no
// global_priv and no account locking.
const mariadbUserSql = `SELECT user, host, is_role, default_role FROM mysql.user`

const mariadbRolesMappingSql = `SELECT role AS from_user, '' AS from_host, user AS to_user, host AS to_host
FROM mysql.roles_mapping`

// userPrivilegesSql lists the grantees the role may see: every account when it
// may read the mysql schema, its own otherwise.
const userPrivilegesSql = `SELECT DISTINCT grantee FROM information_schema.USER_PRIVILEGES`

type mysqlAccountRow struct {
	User          string         `db:"user"`
	Host          string         `db:"host"`
	AccountLocked sql.NullString `db:"account_locked"`
	Comment       sql.NullString `db:"comment"`
}

type mysqlUserAttributesRow struct {
	User      string         `db:"user"`
	Host      string         `db:"host"`
	Attribute sql.NullString `db:"attribute"`
}

type mariadbGlobalPrivRow struct {
	User string         `db:"user"`
	Host string         `db:"host"`
	Priv sql.NullString `db:"priv"`
}

type mariadbUserRow struct {
	User        string         `db:"user"`
	Host        string         `db:"host"`
	IsRole      sql.NullString `db:"is_role"`
	DefaultRole sql.NullString `db:"default_role"`
}

type roleEdgeRow struct {
	FromUser string `db:"from_user"`
	FromHost string `db:"from_host"`
	ToUser   string `db:"to_user"`
	ToHost   string `db:"to_host"`
}

type granteeRow struct {
	Grantee string `db:"grantee"`
}

// account is one user@host row. MySQL authenticates an account, but the same
// user name from several hosts is one login to everything downstream (the
// processlist and performance_schema report USER and HOST apart), so accounts
// are folded into one PlatformUser per user name.
type account struct {
	User         string
	Host         string
	Locked       *bool
	Comment      string
	IsRole       bool
	DefaultRoles []string
}

// Sources of QueryPlatformUsers, named <platform>.<schema>.<table>.
const (
	sourceMySQLUser             = "mysql.mysql.user"
	sourceMySQLUserAttributes   = "mysql.information_schema.user_attributes"
	sourceMariaDBGlobalPriv     = "mariadb.mysql.global_priv"
	sourceMariaDBUser           = "mariadb.mysql.user"
	sourceMariaDBUserPrivileges = "mariadb.information_schema.user_privileges"
)

// QueryPlatformUsers lists the server's accounts, one per user name.
//
// The listing reads the mysql schema. Without SELECT on it, that source is
// reported refused and information_schema is read instead, which shows only
// the connecting account and so is a limited source. information_schema only
// repeats a subset of the mysql schema, so it is read only then. When both are
// refused the mysql schema's permission error is returned. Roles and default
// roles are best effort.
func (e *MySQLScrapper) QueryPlatformUsers(ctx context.Context) (*scrapper.PlatformUsers, error) {
	main, fallback, fallbackSource := e.queryMySQLUsers, e.queryMySQLUserAttributes, sourceMySQLUserAttributes
	if e.isMariaDB {
		main, fallback, fallbackSource = e.queryMariaDBUsers, e.queryGrantees, sourceMariaDBUserPrivileges
	}

	listing, source, err := main(ctx)
	if err == nil {
		return scrapper.NewPlatformUsers(listing), nil
	}
	if !dwhexecmysql.IsPermissionError(err) {
		return nil, errors.Wrapf(err, "failed to read %s", source)
	}
	refused := scrapper.RefusedPlatformUserSource(source, scrapper.PlatformUserSourceSQL, err)

	limited, fallbackErr := fallback(ctx)
	if fallbackErr != nil {
		if !dwhexecmysql.IsPermissionError(fallbackErr) {
			return nil, errors.Wrapf(fallbackErr, "failed to read %s", fallbackSource)
		}
		return nil, errors.Wrapf(err, "failed to read %s", source)
	}
	return scrapper.NewPlatformUsers(refused, limited), nil
}

func (e *MySQLScrapper) queryMySQLUsers(ctx context.Context) (*scrapper.PlatformUserListing, string, error) {
	result := &scrapper.PlatformUserListing{
		Source:       sourceMySQLUser,
		Kind:         scrapper.PlatformUserSourceSQL,
		Completeness: scrapper.PlatformUsersComplete,
	}
	skipFactsNotOnPlatform(result)

	rows, err := selectRows[mysqlAccountRow](ctx, e.executor, mysqlUsersSql)
	if mysqlErrorNumber(err) == errUnknownColumn {
		result.Skip(scrapper.PlatformUserFactComment, "this MySQL version keeps no account comment (added in 8.0.21)")
		rows, err = selectRows[mysqlAccountRow](ctx, e.executor, mysqlUsersNoCommentSql)
	}
	if err != nil {
		return nil, sourceMySQLUser, err
	}

	accounts := make([]*account, 0, len(rows))
	for _, r := range rows {
		accounts = append(accounts, &account{
			User:    r.User,
			Host:    r.Host,
			Locked:  yesNo(r.AccountLocked),
			Comment: r.Comment.String,
		})
	}

	edges, err := selectRows[roleEdgeRow](ctx, e.executor, mysqlRoleEdgesSql)
	if err != nil {
		result.Skip(scrapper.PlatformUserFactRoles, factSkipReason(err, "mysql.role_edges"))
	}
	defaults, err := selectRows[roleEdgeRow](ctx, e.executor, mysqlDefaultRolesSql)
	if err != nil {
		result.Skip(scrapper.PlatformUserFactDefaultRole, factSkipReason(err, "mysql.default_roles"))
	}
	markMySQLRoles(accounts, edges, defaults)

	result.Users = foldAccounts(accounts, edges)
	return result, sourceMySQLUser, nil
}

// markMySQLRoles flags the accounts that are roles and records default roles.
// MySQL keeps a role as a locked account; one that is granted to another
// account or set as someone's default is a role, not a login.
func markMySQLRoles(accounts []*account, edges, defaults []*roleEdgeRow) {
	granted := map[string]bool{}
	for _, e := range edges {
		granted[accountKey(e.FromUser, e.FromHost)] = true
	}
	for _, d := range defaults {
		granted[accountKey(d.FromUser, d.FromHost)] = true
	}
	defaultsOf := map[string][]string{}
	for _, d := range defaults {
		key := accountKey(d.ToUser, d.ToHost)
		defaultsOf[key] = append(defaultsOf[key], d.FromUser)
	}
	for _, a := range accounts {
		key := accountKey(a.User, a.Host)
		a.IsRole = granted[key] && a.Locked != nil && *a.Locked
		a.DefaultRoles = defaultsOf[key]
	}
}

func (e *MySQLScrapper) queryMySQLUserAttributes(ctx context.Context) (*scrapper.PlatformUserListing, error) {
	rows, err := selectRows[mysqlUserAttributesRow](ctx, e.executor, mysqlUserAttributesSql)
	if err != nil {
		return nil, err
	}
	accounts := make([]*account, 0, len(rows))
	for _, r := range rows {
		accounts = append(accounts, &account{User: r.User, Host: r.Host, Comment: attributeComment(r.Attribute)})
	}
	result := limitedListing(sourceMySQLUserAttributes, "information_schema.USER_ATTRIBUTES")
	result.Skip(scrapper.PlatformUserFactDisabled, "reading the account lock needs SELECT on mysql.user")
	result.Skip(scrapper.PlatformUserFactRoles, "reading role grants needs SELECT on mysql.role_edges")
	result.Skip(scrapper.PlatformUserFactDefaultRole, "reading default roles needs SELECT on mysql.default_roles")
	result.Users = foldAccounts(accounts, nil)
	return result, nil
}

func (e *MySQLScrapper) queryMariaDBUsers(ctx context.Context) (*scrapper.PlatformUserListing, string, error) {
	result := &scrapper.PlatformUserListing{
		Source:       sourceMariaDBGlobalPriv,
		Kind:         scrapper.PlatformUserSourceSQL,
		Completeness: scrapper.PlatformUsersComplete,
	}
	skipFactsNotOnPlatform(result)
	result.Skip(scrapper.PlatformUserFactComment, "MariaDB keeps no account comment")

	accounts, err := e.queryMariaDBGlobalPriv(ctx)
	if mysqlErrorNumber(err) == errNoSuchTable {
		result.Source = sourceMariaDBUser
		result.Skip(scrapper.PlatformUserFactDisabled, "this MariaDB version has no account locking (added in 10.4)")
		accounts, err = e.queryMariaDBUserTable(ctx)
	}
	if err != nil {
		return nil, result.Source, err
	}

	edges, err := selectRows[roleEdgeRow](ctx, e.executor, mariadbRolesMappingSql)
	if err != nil {
		result.Skip(scrapper.PlatformUserFactRoles, factSkipReason(err, "mysql.roles_mapping"))
	}
	result.Users = foldAccounts(accounts, edges)
	return result, result.Source, nil
}

// mariadbPriv is the part of a mysql.global_priv document the listing reads.
type mariadbPriv struct {
	AccountLocked bool   `json:"account_locked"`
	IsRole        bool   `json:"is_role"`
	DefaultRole   string `json:"default_role"`
}

func (e *MySQLScrapper) queryMariaDBGlobalPriv(ctx context.Context) ([]*account, error) {
	rows, err := selectRows[mariadbGlobalPrivRow](ctx, e.executor, mariadbGlobalPrivSql)
	if err != nil {
		return nil, err
	}
	accounts := make([]*account, 0, len(rows))
	for _, r := range rows {
		accounts = append(accounts, mariadbAccount(ctx, r))
	}
	return accounts, nil
}

func mariadbAccount(ctx context.Context, r *mariadbGlobalPrivRow) *account {
	a := &account{User: r.User, Host: r.Host}
	var priv mariadbPriv
	if err := json.Unmarshal([]byte(r.Priv.String), &priv); err != nil {
		logging.GetLogger(ctx).WithError(err).WithField("user", r.User).Warn("unreadable mysql.global_priv document")
		return a
	}
	a.Locked = &priv.AccountLocked
	a.IsRole = priv.IsRole
	if priv.DefaultRole != "" {
		a.DefaultRoles = []string{priv.DefaultRole}
	}
	return a
}

func (e *MySQLScrapper) queryMariaDBUserTable(ctx context.Context) ([]*account, error) {
	rows, err := selectRows[mariadbUserRow](ctx, e.executor, mariadbUserSql)
	if err != nil {
		return nil, err
	}
	accounts := make([]*account, 0, len(rows))
	for _, r := range rows {
		a := &account{User: r.User, Host: r.Host, IsRole: strings.EqualFold(r.IsRole.String, "Y")}
		if r.DefaultRole.String != "" {
			a.DefaultRoles = []string{r.DefaultRole.String}
		}
		accounts = append(accounts, a)
	}
	return accounts, nil
}

func (e *MySQLScrapper) queryGrantees(ctx context.Context) (*scrapper.PlatformUserListing, error) {
	rows, err := selectRows[granteeRow](ctx, e.executor, userPrivilegesSql)
	if err != nil {
		return nil, err
	}
	accounts := make([]*account, 0, len(rows))
	for _, r := range rows {
		if user, host, ok := parseGrantee(r.Grantee); ok {
			accounts = append(accounts, &account{User: user, Host: host})
		}
	}
	result := limitedListing(sourceMariaDBUserPrivileges, "information_schema.USER_PRIVILEGES")
	result.Skip(scrapper.PlatformUserFactComment, "MariaDB keeps no account comment")
	result.Skip(scrapper.PlatformUserFactDisabled, "reading the account lock needs SELECT on mysql.global_priv")
	result.Skip(scrapper.PlatformUserFactRoles, "reading role grants needs SELECT on mysql.roles_mapping")
	result.Skip(scrapper.PlatformUserFactDefaultRole, "reading default roles needs SELECT on mysql.global_priv")
	result.Users = foldAccounts(accounts, nil)
	return result, nil
}

func limitedListing(source, view string) *scrapper.PlatformUserListing {
	result := &scrapper.PlatformUserListing{
		Source:       source,
		Kind:         scrapper.PlatformUserSourceSQL,
		Completeness: scrapper.PlatformUsersLimited,
		CompletenessReason: "the role may not read the mysql schema, so " + view +
			" shows only the accounts it may see, usually only its own; grant " + platformUsersGrant,
	}
	skipFactsNotOnPlatform(result)
	return result
}

// foldAccounts turns accounts into one PlatformUser per user name, leaving out
// roles and the server's reserved accounts. A user is disabled only when every
// one of its accounts is locked, since an unlocked host can still sign in.
// edges grant roles: from_user is the role, to_user/to_host the grantee.
func foldAccounts(accounts []*account, edges []*roleEdgeRow) []*scrapper.PlatformUser {
	roles := map[string][]string{}
	for _, e := range edges {
		roles[e.ToUser] = append(roles[e.ToUser], e.FromUser)
	}

	byUser := map[string]*scrapper.PlatformUser{}
	var users []*scrapper.PlatformUser
	for _, a := range accounts {
		if a.IsRole || reservedAccounts[a.User] {
			continue
		}
		u, ok := byUser[a.User]
		if !ok {
			u = &scrapper.PlatformUser{Login: a.User, Roles: roles[a.User]}
			byUser[a.User] = u
			users = append(users, u)
		}
		if u.Comment == "" {
			u.Comment = a.Comment
		}
		if a.Locked != nil {
			disabled := *a.Locked && (u.Disabled == nil || *u.Disabled)
			u.Disabled = &disabled
		}
		if len(a.DefaultRoles) > 0 {
			defaults := append(strings.Split(u.DefaultRole, ","), a.DefaultRoles...)
			defaults = slices.DeleteFunc(defaults, func(s string) bool { return s == "" })
			slices.Sort(defaults)
			u.DefaultRole = strings.Join(slices.Compact(defaults), ",")
		}
	}
	return users
}

// parseGrantee splits an information_schema grantee, 'user'@'host', with a
// quote inside a name written doubled.
func parseGrantee(grantee string) (user, host string, ok bool) {
	if !strings.HasPrefix(grantee, "'") || !strings.HasSuffix(grantee, "'") {
		return "", "", false
	}
	sep := strings.LastIndex(grantee, "'@'")
	if sep <= 0 {
		return "", "", false
	}
	unquote := func(s string) string { return strings.ReplaceAll(s, "''", "'") }
	return unquote(grantee[1:sep]), unquote(grantee[sep+3 : len(grantee)-1]), true
}

// attributeComment reads the comment out of an USER_ATTRIBUTES document.
func attributeComment(attribute sql.NullString) string {
	if !attribute.Valid || attribute.String == "" {
		return ""
	}
	var doc struct {
		Comment string `json:"comment"`
	}
	if err := json.Unmarshal([]byte(attribute.String), &doc); err != nil {
		return ""
	}
	return doc.Comment
}

func yesNo(v sql.NullString) *bool {
	if !v.Valid {
		return nil
	}
	b := strings.EqualFold(v.String, "Y")
	return &b
}

func accountKey(user, host string) string {
	return user + "\x00" + host
}

func skipFactsNotOnPlatform(result *scrapper.PlatformUserListing) {
	for _, f := range mysqlFactsNotOnPlatform {
		result.Skip(f, "MySQL and MariaDB do not record it")
	}
}

func factSkipReason(err error, table string) string {
	if dwhexecmysql.IsPermissionError(err) {
		return "the role may not read " + table + "; grant " + platformUsersGrant
	}
	if mysqlErrorNumber(err) == errNoSuchTable {
		return table + " does not exist on this server version"
	}
	return "reading " + table + " failed: " + err.Error()
}

func mysqlErrorNumber(err error) uint16 {
	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) {
		return mysqlErr.Number
	}
	return 0
}

func selectRows[T any](ctx context.Context, executor *dwhexecmysql.MySQLExecutor, query string) ([]*T, error) {
	rows, err := executor.QueryRows(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scrapper.ScanAll[T](ctx, rows, "mysql platform users")
}
