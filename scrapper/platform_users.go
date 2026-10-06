package scrapper

import (
	"slices"
	"strings"
	"time"
)

// PlatformUsersCapability says whether a platform can list its users, and what
// a role needs to be granted to see all of them. Its zero value means the
// platform has no user listing at all, and QueryPlatformUsers returns
// ErrUnsupported: a caller checks Supported first and never calls it then.
//
// "No listing on this platform" and "a listing our role may not read" are
// different things to a customer. The first is this capability; the second is
// a permission error from QueryPlatformUsers that IsPermissionError recognises.
type PlatformUsersCapability struct {
	// Supported is true when QueryPlatformUsers may return a listing rather
	// than ErrUnsupported.
	Supported bool `json:"supported"`
	// Grant names, in the platform's own terms, what lets the connecting role
	// read every user and every fact, so a caller can quote it in a
	// recommendation ("grant X to see all your users").
	Grant string `json:"grant,omitempty"`
}

// PlatformUserFact names one fact of a PlatformUser, so a listing can say which
// facts it could not read.
type PlatformUserFact string

const (
	PlatformUserFactPlatformId  PlatformUserFact = "platform_id"
	PlatformUserFactType        PlatformUserFact = "type"
	PlatformUserFactEmail       PlatformUserFact = "email"
	PlatformUserFactDisplayName PlatformUserFact = "display_name"
	PlatformUserFactComment     PlatformUserFact = "comment"
	PlatformUserFactDisabled    PlatformUserFact = "disabled"
	PlatformUserFactCreatedAt   PlatformUserFact = "created_at"
	PlatformUserFactLastLoginAt PlatformUserFact = "last_login_at"
	PlatformUserFactDefaultRole PlatformUserFact = "default_role"
	PlatformUserFactRoles       PlatformUserFact = "roles"
)

// PlatformUsersCompleteness says how much of the platform's user list a
// listing holds, as far as the platform lets us tell.
type PlatformUsersCompleteness string

const (
	// PlatformUsersComplete: the listing holds every user of the platform.
	PlatformUsersComplete PlatformUsersCompleteness = "complete"
	// PlatformUsersLimited: the connecting role sees only part of the users.
	// CompletenessReason says why, and names the grant that lifts it.
	PlatformUsersLimited PlatformUsersCompleteness = "limited"
	// PlatformUsersEmpty: the listing worked and returned nobody. Every
	// platform has at least the login we connect with, so this is suspicious
	// and must not be read as "the platform has no users".
	PlatformUsersEmpty PlatformUsersCompleteness = "empty"
	// PlatformUsersUnknown: the platform gives no way to tell whether the
	// listing is complete.
	PlatformUsersUnknown PlatformUsersCompleteness = "unknown"
)

// Values of PlatformUser.Type on the platforms that state one. The type is
// stored as the platform states it, never derived: "looks like a service
// account" is the caller's suggestion, not a fact of the platform.
const (
	// Snowflake USERS.TYPE. A user created before TYPE existed has none.
	PlatformUserTypeSnowflakePerson        = "PERSON"
	PlatformUserTypeSnowflakeService       = "SERVICE"
	PlatformUserTypeSnowflakeLegacyService = "LEGACY_SERVICE"

	// Databricks SCIM: a workspace user, or a service principal whose Login is
	// its application id (what query history reports as the user).
	PlatformUserTypeDatabricksUser             = "user"
	PlatformUserTypeDatabricksServicePrincipal = "service_principal"

	// BigQuery IAM member prefixes: an address under gserviceaccount.com is a
	// serviceAccount, a person a user.
	PlatformUserTypeBigQueryUser           = "user"
	PlatformUserTypeBigQueryServiceAccount = "serviceAccount"
)

// PlatformUser is one login on a platform instance, with every fact the
// platform states about it. Only Login is always set; every other field is
// best effort and stays empty when the platform does not state it or the
// connecting role may not read it (the listing's SkippedFacts says which).
type PlatformUser struct {
	// Login is the name the platform authenticates, as the platform shows it.
	// It is the same string query history reports as the user who ran a
	// statement, so the two can be joined.
	Login string `json:"login"`
	// PlatformId is the platform's own stable id for the user, where it has
	// one that survives a rename (Snowflake USER_ID, Databricks SCIM id,
	// Postgres oid).
	PlatformId string `json:"platform_id,omitempty"`
	// Type is the kind of login as the platform states it; see the
	// PlatformUserType* constants for the platforms that have one.
	Type string `json:"type,omitempty"`
	// Email the platform holds for the user.
	Email string `json:"email,omitempty"`
	// DisplayName is the user's name for humans, where it differs from Login.
	DisplayName string `json:"display_name,omitempty"`
	// Comment is the free-text description an administrator left on the user.
	// It often says what a service login is for.
	Comment string `json:"comment,omitempty"`
	// Disabled is true when the login exists but cannot sign in (disabled,
	// locked or expired, as the platform reports it). Nil when unknown.
	Disabled *bool `json:"disabled,omitempty"`
	// CreatedAt is when the user was created. Nil when unknown.
	CreatedAt *time.Time `json:"created_at,omitempty"`
	// LastLoginAt is the user's last successful sign-in. Nil when unknown or
	// never.
	LastLoginAt *time.Time `json:"last_login_at,omitempty"`
	// DefaultRole is the role a session starts with.
	DefaultRole string `json:"default_role,omitempty"`
	// Roles are the roles or groups granted to the user directly, sorted and
	// without duplicates. Roles say a lot about what a login is for
	// (LOADER, TRANSFORMER, REPORTER).
	Roles []string `json:"roles,omitempty"`
}

// SkippedPlatformUserFact says that a fact could not be read, and why: the
// platform does not have it, or our role may not read it.
type SkippedPlatformUserFact struct {
	Fact   PlatformUserFact `json:"fact"`
	Reason string           `json:"reason"`
}

// PlatformUserSourceKind says how a source of platform users was read.
type PlatformUserSourceKind string

const (
	// PlatformUserSourceSQL is a catalog view or command read over the
	// warehouse connection.
	PlatformUserSourceSQL PlatformUserSourceKind = "sql"
	// PlatformUserSourceAPI is the platform's REST API (Databricks SCIM, Google
	// IAM, Fabric).
	PlatformUserSourceAPI PlatformUserSourceKind = "api"
)

// PlatformUserListing is what one source said about the platform's users, or,
// from PlatformUsers.Reconcile, what all of them said together.
type PlatformUserListing struct {
	// Source names where the listing was read, as <platform>.<source>
	// ("snowflake.account_usage.users", "bigquery.iam_policy"). A reconciled
	// listing has PlatformUsersReconciledSource.
	Source string `json:"source"`
	// Kind says whether the source is SQL or an API. Empty on a reconciled
	// listing.
	Kind PlatformUserSourceKind `json:"kind,omitempty"`
	// Refused is set when the source refused the connecting role, with the
	// platform's error. Such a listing holds no users and its completeness
	// means nothing.
	Refused string `json:"refused,omitempty"`
	// Users sorted by Login, one per login.
	Users []*PlatformUser `json:"users"`
	// Completeness says how much of the platform's users the listing holds.
	Completeness PlatformUsersCompleteness `json:"completeness,omitempty"`
	// CompletenessReason explains a limited, empty or unknown listing, and
	// names the grant that would complete it where one would.
	CompletenessReason string `json:"completeness_reason,omitempty"`
	// SkippedFacts lists the facts the listing could not read in full, with
	// the reason: absent on the platform, refused for every user, hidden for
	// the users the role may not see the details of (Snowflake SHOW USERS), or
	// read only in part (Redshift group roles without its RBAC roles). A fact
	// a user simply does not have (a user without an email) is not listed.
	SkippedFacts []SkippedPlatformUserFact `json:"skipped_facts,omitempty"`
}

// PlatformUsersReconciledSource is the Source of a listing Reconcile made.
const PlatformUsersReconciledSource = "reconciled"

// PlatformUsers is the result of QueryPlatformUsers: what each source the
// platform has said, kept apart so a caller can weigh one against another
// (the warehouse's own view against the platform's API, a complete source
// against a fresher one). Reconcile merges them for a caller that wants one
// list.
//
// A platform reads every source that can add a user or a fact. A fallback
// that only repeats a subset of another source (Oracle ALL_USERS next to
// DBA_USERS) is read when that source refused, and the refusal is kept as a
// source of its own, so a caller sees what was missed and why.
type PlatformUsers struct {
	// Sources in the order the platform trusts them, most trusted first:
	// Reconcile takes a fact from the first source that has it.
	Sources []*PlatformUserListing `json:"sources"`
}

// NewPlatformUsers finishes each source (see PlatformUserListing.Finish) and
// returns them as one result.
func NewPlatformUsers(sources ...*PlatformUserListing) *PlatformUsers {
	result := &PlatformUsers{}
	for _, src := range sources {
		if src == nil {
			continue
		}
		result.Sources = append(result.Sources, src.Finish())
	}
	return result
}

// RefusedPlatformUserSource records a source that refused the connecting role.
func RefusedPlatformUserSource(source string, kind PlatformUserSourceKind, err error) *PlatformUserListing {
	return &PlatformUserListing{Source: source, Kind: kind, Refused: err.Error()}
}

// Source returns the listing of the named source, or nil.
func (p *PlatformUsers) Source(name string) *PlatformUserListing {
	if p == nil {
		return nil
	}
	for _, src := range p.Sources {
		if src.Source == name {
			return src
		}
	}
	return nil
}

// AllRefused reports whether no source could be read. QueryPlatformUsers
// returns a permission error then instead of a result.
func (p *PlatformUsers) AllRefused() bool {
	for _, src := range p.Sources {
		if src.Refused == "" {
			return false
		}
	}
	return true
}

// Reconcile merges the sources into one listing. It is the default reading,
// for a caller that has no reason to weigh the sources itself:
//
//   - a user is listed when any source lists it;
//   - each fact is taken from the first source, in trust order, that has it,
//     and roles are united across sources;
//   - the listing is as complete as its most complete source, since a
//     complete source saw every user: complete, else limited, else unknown;
//   - a fact is skipped only when every source that answered skipped it;
//   - a refused source adds its refusal to the reason of a listing that is
//     not complete.
func (p *PlatformUsers) Reconcile() *PlatformUserListing {
	result := &PlatformUserListing{Source: PlatformUsersReconciledSource}
	if p == nil {
		return result.Finish()
	}

	answered := make([]*PlatformUserListing, 0, len(p.Sources))
	var refusals []string
	for _, src := range p.Sources {
		if src.Refused != "" {
			refusals = append(refusals, src.Source+" refused: "+src.Refused)
			continue
		}
		answered = append(answered, src)
	}

	byLogin := map[string]*PlatformUser{}
	for _, src := range answered {
		for _, u := range src.Users {
			if seen, ok := byLogin[u.Login]; ok {
				seen.merge(u)
				continue
			}
			c := u.clone()
			byLogin[u.Login] = c
			result.Users = append(result.Users, c)
		}
	}

	result.Completeness, result.CompletenessReason = reconcileCompleteness(answered, refusals)

	for _, fact := range allPlatformUserFacts {
		var reasons []string
		for _, src := range answered {
			reason, skipped := src.skipReason(fact)
			if !skipped {
				reasons = nil
				break
			}
			reasons = append(reasons, src.Source+": "+reason)
		}
		if len(reasons) > 0 {
			result.Skip(fact, strings.Join(reasons, "; "))
		}
	}
	return result.Finish()
}

func reconcileCompleteness(answered []*PlatformUserListing, refusals []string) (PlatformUsersCompleteness, string) {
	for _, want := range []PlatformUsersCompleteness{PlatformUsersComplete, PlatformUsersLimited, PlatformUsersUnknown} {
		var reasons []string
		found := false
		for _, src := range answered {
			if src.Completeness != want {
				continue
			}
			found = true
			if src.CompletenessReason != "" {
				reasons = append(reasons, src.Source+": "+src.CompletenessReason)
			}
		}
		if !found {
			continue
		}
		if want == PlatformUsersComplete {
			return want, ""
		}
		return want, strings.Join(append(reasons, refusals...), "; ")
	}
	// Every source that answered was empty, or none answered.
	return PlatformUsersEmpty, strings.Join(refusals, "; ")
}

var allPlatformUserFacts = []PlatformUserFact{
	PlatformUserFactPlatformId,
	PlatformUserFactType,
	PlatformUserFactEmail,
	PlatformUserFactDisplayName,
	PlatformUserFactComment,
	PlatformUserFactDisabled,
	PlatformUserFactCreatedAt,
	PlatformUserFactLastLoginAt,
	PlatformUserFactDefaultRole,
	PlatformUserFactRoles,
}

func (p *PlatformUserListing) skipReason(fact PlatformUserFact) (string, bool) {
	for _, s := range p.SkippedFacts {
		if s.Fact == fact {
			return s.Reason, true
		}
	}
	return "", false
}

// Skip records that fact could not be read in full. A fact skipped twice
// keeps its first reason.
func (p *PlatformUserListing) Skip(fact PlatformUserFact, reason string) {
	for _, s := range p.SkippedFacts {
		if s.Fact == fact {
			return
		}
	}
	p.SkippedFacts = append(p.SkippedFacts, SkippedPlatformUserFact{Fact: fact, Reason: reason})
}

// IsSkipped reports whether fact was recorded as skipped.
func (p *PlatformUserListing) IsSkipped(fact PlatformUserFact) bool {
	for _, s := range p.SkippedFacts {
		if s.Fact == fact {
			return true
		}
	}
	return false
}

// Finish puts a listing into its final shape: users without a login are
// dropped, users sharing a login are merged (the first non-empty value of each
// fact wins, roles are united), roles are sorted and deduplicated, users are
// sorted by login, and an empty listing is marked PlatformUsersEmpty whatever
// completeness was claimed. Every implementation returns through it.
func (p *PlatformUserListing) Finish() *PlatformUserListing {
	byLogin := make(map[string]*PlatformUser, len(p.Users))
	users := make([]*PlatformUser, 0, len(p.Users))
	for _, u := range p.Users {
		if u == nil || strings.TrimSpace(u.Login) == "" {
			continue
		}
		if seen, ok := byLogin[u.Login]; ok {
			seen.merge(u)
			continue
		}
		byLogin[u.Login] = u
		users = append(users, u)
	}
	for _, u := range users {
		u.Roles = sortedUnique(u.Roles)
	}
	slices.SortFunc(users, func(a, b *PlatformUser) int { return strings.Compare(a.Login, b.Login) })
	p.Users = users
	slices.SortFunc(p.SkippedFacts, func(a, b SkippedPlatformUserFact) int { return strings.Compare(string(a.Fact), string(b.Fact)) })

	if p.Refused != "" {
		p.Completeness = ""
		return p
	}
	if len(p.Users) == 0 {
		p.Completeness = PlatformUsersEmpty
		if p.CompletenessReason == "" {
			p.CompletenessReason = "the platform returned no users, not even the one we connect as"
		}
	}
	if p.Completeness == "" {
		p.Completeness = PlatformUsersUnknown
	}
	return p
}

func (u *PlatformUser) merge(o *PlatformUser) {
	firstString := func(dst *string, src string) {
		if *dst == "" {
			*dst = src
		}
	}
	firstString(&u.PlatformId, o.PlatformId)
	firstString(&u.Type, o.Type)
	firstString(&u.Email, o.Email)
	firstString(&u.DisplayName, o.DisplayName)
	firstString(&u.Comment, o.Comment)
	firstString(&u.DefaultRole, o.DefaultRole)
	if u.Disabled == nil {
		u.Disabled = o.Disabled
	}
	if u.CreatedAt == nil {
		u.CreatedAt = o.CreatedAt
	}
	if u.LastLoginAt == nil {
		u.LastLoginAt = o.LastLoginAt
	}
	u.Roles = append(u.Roles, o.Roles...)
}

func (u *PlatformUser) clone() *PlatformUser {
	c := *u
	c.Roles = slices.Clone(u.Roles)
	return &c
}

func sortedUnique(values []string) []string {
	out := make([]string, 0, len(values))
	for _, v := range values {
		if v != "" {
			out = append(out, v)
		}
	}
	if len(out) == 0 {
		return nil
	}
	slices.Sort(out)
	return slices.Compact(out)
}

// AssignRoles adds roles to the users they belong to, keyed by login. Roles of
// a login that is not in the listing are dropped: a grant to a user we did not
// list does not add a user.
func (p *PlatformUserListing) AssignRoles(rolesByLogin map[string][]string) {
	for _, u := range p.Users {
		u.Roles = append(u.Roles, rolesByLogin[u.Login]...)
	}
}

// Sanitize cleans every string of every source (see SanitizeString).
func (p *PlatformUsers) Sanitize() {
	if p == nil {
		return
	}
	for _, src := range p.Sources {
		src.Sanitize()
	}
}

// Sanitize cleans every string of the listing (see SanitizeString).
func (p *PlatformUserListing) Sanitize() {
	if p == nil {
		return
	}
	for _, u := range p.Users {
		u.Sanitize()
	}
	p.Refused = SanitizeString(p.Refused)
	p.CompletenessReason = SanitizeString(p.CompletenessReason)
	for i := range p.SkippedFacts {
		p.SkippedFacts[i].Reason = SanitizeString(p.SkippedFacts[i].Reason)
	}
}

func (u *PlatformUser) Sanitize() {
	if u == nil {
		return
	}
	u.Login = SanitizeString(u.Login)
	u.PlatformId = SanitizeString(u.PlatformId)
	u.Type = SanitizeString(u.Type)
	u.Email = SanitizeString(u.Email)
	u.DisplayName = SanitizeString(u.DisplayName)
	u.Comment = SanitizeString(u.Comment)
	u.DefaultRole = SanitizeString(u.DefaultRole)
	for i := range u.Roles {
		u.Roles[i] = SanitizeString(u.Roles[i])
	}
}

// HasValidIdentity reports whether the login is usable as an identity: set,
// free of NUL bytes and valid UTF-8. A repaired login could name another user,
// so the rejecting decorator drops such a user instead of sanitizing it.
func (u *PlatformUser) HasValidIdentity() bool {
	return u != nil && u.Login != "" && IsValidString(u.Login)
}
