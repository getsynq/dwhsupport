package fabric

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	dwhexecfabric "github.com/getsynq/dwhsupport/exec/fabric"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

// The tables of a Lakehouse, a Warehouse, a mirrored database and a SQL
// database are Delta tables in OneLake, at Tables/<schema>/<table>, or at
// Tables/<table> in a Lakehouse with no schemas (where SQL shows them in dbo).
// A commit of a Delta table is the file _delta_log/<version>.json, and the
// Delta protocol makes the time the file was written the time of the commit,
// unless the commit carries an in-commit timestamp.

// maxCommitsForLastChange bounds how many commits, newest first, are read to
// find the last one that changed data. Commits that change no data (a
// compaction, a change of the table properties) do not move the last change.
const maxCommitsForLastChange = 20

// maxDeltaLogConcurrency bounds the tables whose Delta log the reader reads at
// the same time, over all databases of the scrapper. A table is three OneLake
// requests in sequence, and a request takes about 150 ms, so a database of 500
// tables takes about 7 s.
const maxDeltaLogConcurrency = 32

var commitFileName = regexp.MustCompile(`^(\d{20})\.json$`)

// oneLake is the part of dwhexecfabric.OneLakeClient the reader uses.
type oneLake interface {
	ListDirectory(ctx context.Context, workspaceID, itemID, directory string, opts dwhexecfabric.ListOptions) ([]*dwhexecfabric.OneLakePath, error)
	ReadFile(ctx context.Context, workspaceID, itemID, path string) ([]byte, error)
}

// storageItems is the part of dwhexecfabric.APIClient the reader uses.
type storageItems interface {
	ListStorageItems(ctx context.Context, workspaceID string) ([]*dwhexecfabric.StorageItem, error)
}

// deltaLogReader reads the Delta logs of the tables of one workspace.
type deltaLogReader struct {
	workspaceID string
	lake        oneLake
	items       storageItems
	// slots holds one token for each table that is read, so the databases that
	// QueryTableMetrics reads at the same time share maxDeltaLogConcurrency.
	slots chan struct{}

	// itemsByDB is the storage items listing, read one time for each scrapper.
	// A scrapper serves one run, so a new database is in the next run.
	mu        sync.Mutex
	itemsByDB map[string]*dwhexecfabric.StorageItem
}

// newDeltaLogReader builds the reader with the REST client of the scrapper, so
// all calls of the scrapper share one client.
func newDeltaLogReader(conf *FabricScrapperConf, api storageItems) (*deltaLogReader, error) {
	identity, err := dwhexecfabric.ParseHostIdentity(conf.Host)
	if err != nil {
		return nil, errors.Wrap(err, "the host names no Fabric workspace")
	}
	lake, err := dwhexecfabric.NewOneLakeClient(&conf.FabricConf)
	if err != nil {
		return nil, err
	}
	return &deltaLogReader{workspaceID: identity.WorkspaceID, lake: lake, items: api, slots: make(chan struct{}, maxDeltaLogConcurrency)}, nil
}

// itemOf returns the storage item that holds the tables of database. A
// database that no storage item names, or that two storage items name, has no
// item: the SQL endpoint cannot tell two such items apart either. A failed
// listing is not kept, so the next call lists again.
func (r *deltaLogReader) itemOf(ctx context.Context, database string) (*dwhexecfabric.StorageItem, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.itemsByDB == nil {
		items, err := r.items.ListStorageItems(ctx, r.workspaceID)
		if err != nil {
			return nil, false, err
		}
		r.itemsByDB = storageItemsByName(items)
	}
	item := r.itemsByDB[database]
	return item, item != nil, nil
}

// storageItemsByName maps each display name to its storage item. A name two
// storage items share maps to nil.
func storageItemsByName(items []*dwhexecfabric.StorageItem) map[string]*dwhexecfabric.StorageItem {
	out := map[string]*dwhexecfabric.StorageItem{}
	for _, it := range items {
		if _, seen := out[it.DisplayName]; seen {
			out[it.DisplayName] = nil
			continue
		}
		out[it.DisplayName] = it
	}
	return out
}

// tableDirs gives the directories the table can be in, the likely one first.
// A dbo table is at Tables/<table> in a Lakehouse with no schemas
// (StorageItem.HasSchemas is false). The other directory is the fallback.
func tableDirs(hasSchemas bool, schema, table string) []string {
	withSchema := "Tables/" + schema + "/" + table
	if !strings.EqualFold(schema, "dbo") {
		return []string{withSchema}
	}
	noSchema := "Tables/" + table
	if !hasSchemas {
		return []string{noSchema, withSchema}
	}
	return []string{withSchema, noSchema}
}

// deltaCommitFile is one commit file of a Delta log.
type deltaCommitFile struct {
	Version int64
	Name    string
	Written time.Time
}

// versionPrefix is the start of the name of each file of a version: its commit,
// its CRC and its checkpoint.
func versionPrefix(version int64) string { return fmt.Sprintf("%020d", version) }

// commitFiles keeps the commit files of a listing of a Delta log, newest first.
func commitFiles(files []*dwhexecfabric.OneLakePath) []*deltaCommitFile {
	var commits []*deltaCommitFile
	for _, f := range files {
		m := commitFileName.FindStringSubmatch(f.Name)
		if f.IsDirectory || m == nil {
			continue
		}
		version, err := strconv.ParseInt(m[1], 10, 64)
		if err != nil {
			continue
		}
		commits = append(commits, &deltaCommitFile{Version: version, Name: f.Name, Written: f.LastModified})
	}
	sort.Slice(commits, func(i, j int) bool { return commits[i].Version > commits[j].Version })
	return commits
}

// findLog calls list for each directory the table can be in, and returns the
// first directory whose Delta log exists. errNoDeltaLog means no directory
// holds one.
func findLog(dirs []string, list func(logDir string) ([]*deltaCommitFile, error)) (string, []*deltaCommitFile, error) {
	for _, dir := range dirs {
		commits, err := list(dir + "/_delta_log")
		if dwhexecfabric.IsOneLakeNotFound(err) {
			continue
		}
		if err != nil {
			return "", nil, err
		}
		return dir, commits, nil
	}
	return "", nil, errNoDeltaLog
}

// commitsOf lists all commit files of the table, newest first.
func (r *deltaLogReader) commitsOf(ctx context.Context, itemID string, dirs []string) (string, []*deltaCommitFile, error) {
	return findLog(dirs, func(logDir string) ([]*deltaCommitFile, error) {
		files, err := r.lake.ListDirectory(ctx, r.workspaceID, itemID, logDir, dwhexecfabric.ListOptions{})
		return commitFiles(files), err
	})
}

// recentCommitsOf lists the commit files from maxCommitsForLastChange versions
// before the last checkpoint to the end of the log, newest first. A log keeps
// its commits for 30 days by default, so a table that loads often has
// thousands of them. Both Spark and the Warehouse write a checkpoint at least
// every few hundred commits (Spark every 10, the Warehouse about every 100), so
// this is one listing page. If the checkpoint is past the end of the log (the
// table was created again), it lists from version 0.
func (r *deltaLogReader) recentCommitsOf(ctx context.Context, itemID string, dirs []string) (string, []*deltaCommitFile, error) {
	return findLog(dirs, func(logDir string) ([]*deltaCommitFile, error) {
		begin := max(0, r.checkpointVersion(ctx, itemID, logDir)-maxCommitsForLastChange+1)
		for {
			files, err := r.lake.ListDirectory(ctx, r.workspaceID, itemID, logDir, dwhexecfabric.ListOptions{BeginFrom: versionPrefix(begin)})
			if err != nil {
				return nil, err
			}
			commits := commitFiles(files)
			if len(commits) > 0 || begin == 0 {
				return commits, nil
			}
			begin = 0
		}
	})
}

// checkpointVersion is the version of the last checkpoint of the log, or 0 if
// the log has none or it cannot be read. A refusal shows in the listing.
func (r *deltaLogReader) checkpointVersion(ctx context.Context, itemID, logDir string) int64 {
	body, err := r.lake.ReadFile(ctx, r.workspaceID, itemID, logDir+"/_last_checkpoint")
	if err != nil {
		return 0
	}
	var checkpoint struct {
		Version int64 `json:"version"`
	}
	if json.Unmarshal(body, &checkpoint) != nil || checkpoint.Version < 0 {
		return 0
	}
	return checkpoint.Version
}

var errNoDeltaLog = errors.New("the table has no Delta log in OneLake")

func (r *deltaLogReader) readCommit(ctx context.Context, itemID, dir string, file *deltaCommitFile) (*deltaCommit, error) {
	body, err := r.lake.ReadFile(ctx, r.workspaceID, itemID, dir+"/_delta_log/"+file.Name)
	if err != nil {
		return nil, err
	}
	return parseDeltaCommit(file.Version, file.Written, body)
}

// lastChange returns the time of the newest commit that changed data. If the
// log reaches its first version with no such commit, the table was created and
// never written, and the time of its creation is the last change. It returns
// nil if the newest maxCommitsForLastChange commits change no data and older
// ones exist.
func (r *deltaLogReader) lastChange(ctx context.Context, itemID string, dirs []string) (*time.Time, error) {
	select {
	case r.slots <- struct{}{}:
		defer func() { <-r.slots }()
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	dir, commits, err := r.recentCommitsOf(ctx, itemID, dirs)
	if err != nil {
		return nil, err
	}
	for i, file := range commits {
		if i == maxCommitsForLastChange {
			return nil, nil
		}
		commit, err := r.readCommit(ctx, itemID, dir, file)
		if err != nil {
			return nil, err
		}
		if commit.DataChange || file.Version == 0 {
			t := commit.Time
			return &t, nil
		}
	}
	return nil, nil
}

// commitTimeSlack is how far the time of a commit can be from the time its file
// was written. An in-commit timestamp is set before the file is written.
const commitTimeSlack = time.Minute

// history returns the commits that changed data with a commit time in [from, to],
// newest first, at most limit of them. The file times select the candidates,
// with commitTimeSlack on each side, and the commit time decides.
func (r *deltaLogReader) history(ctx context.Context, itemID string, dirs []string, from, to time.Time, limit int) ([]*deltaCommit, error) {
	if limit <= 0 {
		return nil, nil
	}
	dir, commits, err := r.commitsOf(ctx, itemID, dirs)
	if err != nil {
		return nil, err
	}
	var inRange []*deltaCommitFile
	for _, c := range commits {
		if !c.Written.Before(from.Add(-commitTimeSlack)) && !c.Written.After(to.Add(commitTimeSlack)) {
			inRange = append(inRange, c)
		}
	}

	var out []*deltaCommit
	for start := 0; start < len(inRange) && len(out) < limit; start += limit {
		end := min(start+limit, len(inRange))
		batch := make([]*deltaCommit, end-start)
		g, gctx := errgroup.WithContext(ctx)
		g.SetLimit(maxDeltaLogConcurrency)
		for i, file := range inRange[start:end] {
			g.Go(func() error {
				commit, err := r.readCommit(gctx, itemID, dir, file)
				batch[i] = commit
				return err
			})
		}
		if err := g.Wait(); err != nil {
			return nil, err
		}
		for _, c := range batch {
			if c.DataChange && !c.Time.Before(from) && !c.Time.After(to) && len(out) < limit {
				out = append(out, c)
			}
		}
	}
	return out, nil
}

// deltaCommit is what one commit of a Delta table did.
type deltaCommit struct {
	Version int64
	Time    time.Time
	// DataChange is true if the commit added or removed data. A compaction or a
	// change of the table metadata is not a data change.
	DataChange bool
	// Operation is one of the scrapper.TableChangeEvent operations.
	Operation    string
	RowsInserted *int64
	RowsUpdated  *int64
	RowsDeleted  *int64
}

type deltaAction struct {
	Add        *deltaFileAction `json:"add"`
	Remove     *deltaFileAction `json:"remove"`
	CommitInfo *deltaCommitInfo `json:"commitInfo"`
}

type deltaFileAction struct {
	Path           string `json:"path"`
	DataChange     bool   `json:"dataChange"`
	Stats          string `json:"stats"`
	DeletionVector *struct {
		Cardinality int64 `json:"cardinality"`
	} `json:"deletionVector"`
}

// liveRows is the number of rows of the file the action leaves visible: its
// records less the rows its deletion vector marks deleted. ok is false if the
// action has no record count.
func (a *deltaFileAction) liveRows() (int64, bool) {
	if a.Stats == "" {
		return 0, false
	}
	var stats struct {
		NumRecords *int64 `json:"numRecords"`
	}
	if json.Unmarshal([]byte(a.Stats), &stats) != nil || stats.NumRecords == nil {
		return 0, false
	}
	return *stats.NumRecords - a.deletedRows(), true
}

func (a *deltaFileAction) deletedRows() int64 {
	if a.DeletionVector == nil {
		return 0
	}
	return a.DeletionVector.Cardinality
}

type deltaCommitInfo struct {
	Operation         string                     `json:"operation"`
	OperationMetrics  map[string]json.RawMessage `json:"operationMetrics"`
	InCommitTimestamp *int64                     `json:"inCommitTimestamp"`
}

func (ci *deltaCommitInfo) metric(names ...string) *int64 {
	for _, name := range names {
		raw, ok := ci.OperationMetrics[name]
		if !ok {
			continue
		}
		text := strings.Trim(string(raw), `"`)
		if n, err := strconv.ParseInt(text, 10, 64); err == nil {
			return &n
		}
	}
	return nil
}

// parseDeltaCommit reads one commit file, a JSON object per line.
//
// A Spark writer records what the operation did in commitInfo.operationMetrics,
// and that is used as it is. Other writers (the Warehouse engine, a Data Factory
// Copy) record no metrics, and the Warehouse no commitInfo at all, so the rows
// come from the actions: a new file adds its live rows, a deletion vector that
// grows on a file deletes rows, and a removed file deletes its live rows. A
// count stays nil if one action of its kind has no record count. Without metrics
// the actions cannot tell an UPDATE or a MERGE from a DELETE and an INSERT, so a
// commit that does both is OTHER, with the rows it inserted and deleted.
func parseDeltaCommit(version int64, written time.Time, body []byte) (*deltaCommit, error) {
	commit := &deltaCommit{Version: version, Time: written}
	var adds, removes []*deltaFileAction
	var info *deltaCommitInfo

	scanner := bufio.NewScanner(bytes.NewReader(body))
	scanner.Buffer(make([]byte, 0, 64*1024), 64*1024*1024)
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		var action deltaAction
		if err := json.Unmarshal(line, &action); err != nil {
			return nil, errors.Wrapf(err, "decoding commit %d", version)
		}
		switch {
		case action.Add != nil:
			if action.Add.DataChange {
				adds = append(adds, action.Add)
			}
		case action.Remove != nil:
			if action.Remove.DataChange {
				removes = append(removes, action.Remove)
			}
		case action.CommitInfo != nil:
			info = action.CommitInfo
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, errors.Wrapf(err, "reading commit %d", version)
	}

	if info != nil && info.InCommitTimestamp != nil {
		commit.Time = time.UnixMilli(*info.InCommitTimestamp).UTC()
	}
	commit.DataChange = len(adds) > 0 || len(removes) > 0
	if !commit.DataChange {
		commit.Operation = "OTHER"
		return commit, nil
	}

	if info != nil && len(info.OperationMetrics) > 0 {
		commit.Operation = normalizeDeltaOperation(info.Operation)
		commit.RowsInserted = info.metric("numTargetRowsInserted")
		if commit.RowsInserted == nil && (commit.Operation == "INSERT" || commit.Operation == "COPY") {
			commit.RowsInserted = info.metric("numOutputRows")
		}
		commit.RowsUpdated = info.metric("numTargetRowsUpdated", "numUpdatedRows")
		commit.RowsDeleted = info.metric("numTargetRowsDeleted", "numDeletedRows")
		return commit, nil
	}

	commit.RowsInserted, commit.RowsDeleted = rowsFromActions(adds, removes)
	inserts, deletes := hasNewFile(adds, removes), hasRemoval(adds, removes)
	switch {
	case inserts && !deletes:
		commit.Operation = "INSERT"
	case deletes && !inserts:
		commit.Operation = "DELETE"
	default:
		commit.Operation = "OTHER"
	}
	return commit, nil
}

// hasNewFile reports whether the commit adds a file it does not also remove.
func hasNewFile(adds, removes []*deltaFileAction) bool {
	removed := pathsOf(removes)
	for _, a := range adds {
		if !removed[a.Path] {
			return true
		}
	}
	return false
}

// hasRemoval reports whether the commit removes rows: a file it removes for
// good, or a file whose deletion vector it grows.
func hasRemoval(adds, removes []*deltaFileAction) bool {
	readded := map[string]*deltaFileAction{}
	for _, a := range adds {
		readded[a.Path] = a
	}
	for _, rm := range removes {
		a, ok := readded[rm.Path]
		if !ok || a.deletedRows() > rm.deletedRows() {
			return true
		}
	}
	return false
}

func pathsOf(actions []*deltaFileAction) map[string]bool {
	out := map[string]bool{}
	for _, a := range actions {
		out[a.Path] = true
	}
	return out
}

// rowsFromActions counts the rows the actions insert and delete. A file that
// is removed and added again in one commit is a file whose deletion vector
// changed: the rows its vector gained are deleted. A count is nil if an action
// it needs has no record count, and also nil if the commit has no action of its
// kind.
func rowsFromActions(adds, removes []*deltaFileAction) (inserted, deleted *int64) {
	removed := map[string]*deltaFileAction{}
	for _, rm := range removes {
		removed[rm.Path] = rm
	}
	readded := map[string]bool{}

	var ins, del int64
	insKnown, delKnown := true, true
	var anyIns, anyDel bool
	for _, a := range adds {
		if rm, ok := removed[a.Path]; ok {
			readded[a.Path] = true
			if gained := a.deletedRows() - rm.deletedRows(); gained > 0 {
				anyDel = true
				del += gained
			}
			continue
		}
		anyIns = true
		n, ok := a.liveRows()
		if !ok {
			insKnown = false
		}
		ins += n
	}
	for _, rm := range removes {
		if readded[rm.Path] {
			continue
		}
		anyDel = true
		n, ok := rm.liveRows()
		if !ok {
			delKnown = false
		}
		del += n
	}
	if anyIns && insKnown {
		inserted = &ins
	}
	if anyDel && delKnown {
		deleted = &del
	}
	return inserted, deleted
}

// normalizeDeltaOperation maps the operation a Spark writer records to a
// scrapper.TableChangeEvent operation.
func normalizeDeltaOperation(op string) string {
	switch strings.ToUpper(op) {
	case "WRITE", "STREAMING UPDATE", "CREATE TABLE AS SELECT", "REPLACE TABLE AS SELECT", "CREATE OR REPLACE TABLE AS SELECT":
		return "INSERT"
	case "COPY INTO":
		return "COPY"
	case "MERGE":
		return "MERGE"
	case "UPDATE":
		return "UPDATE"
	case "DELETE":
		return "DELETE"
	case "TRUNCATE":
		return "TRUNCATE"
	default:
		return "OTHER"
	}
}
