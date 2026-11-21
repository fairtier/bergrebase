package test

import (
	"bytes"
	"context"
	"fmt"
	"maps"
	"net/http"
	"os"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"

	iceberg "github.com/apache/iceberg-go"

	bergstorage "github.com/fairtier/bergrebase/internal/storage"
	"github.com/fairtier/bergrebase/test/harness"
)

// These tests are the ones with real writers. DuckDB and pyiceberg commit
// through a real Lakekeeper, the real bergrb binary moves the tables, the old
// objects are deleted, and the result is read back through the catalog. The
// rest of the suite seeds hand-built fixtures that real writers do not produce:
// uncompressed metadata, complete Avro headers, small snapshot IDs, only ADDED
// entries.

const realColumns = "id BIGINT, g INT, s VARCHAR, d DATE, ts TIMESTAMP"

func realRows(lo, hi int) string {
	return fmt.Sprintf(`SELECT i::BIGINT AS id, (i%%10)::INT AS g, 'v'||i AS s,
		DATE '2026-01-01' + (i%%40)::INT AS d, TIMESTAMP '2026-01-01' + INTERVAL (i) HOUR AS ts
		FROM range(%d, %d) r(i)`, lo, hi)
}

// duckDBShapes writes the table shapes and returns each table's expected count.
func duckDBShapes(t *testing.T, d harness.DuckDB, s *harness.Stack, ns string) map[string]int64 {
	t.Helper()
	q := func(table string) string { return "lk." + ns + "." + table }
	d.Exec(t, s,
		"CREATE SCHEMA lk."+ns,

		fmt.Sprintf("CREATE TABLE %s (%s)", q("empty"), realColumns),

		fmt.Sprintf("CREATE TABLE %s (%s)", q("inserts"), realColumns),
		fmt.Sprintf("INSERT INTO %s %s", q("inserts"), realRows(1, 1001)),
		fmt.Sprintf("INSERT INTO %s %s", q("inserts"), realRows(1001, 1101)),

		fmt.Sprintf("CREATE TABLE %s (%s)", q("deletes"), realColumns),
		fmt.Sprintf("INSERT INTO %s %s", q("deletes"), realRows(1, 1001)),
		"DELETE FROM "+q("deletes")+" WHERE id % 10 = 0",

		fmt.Sprintf("CREATE TABLE %s (%s)", q("dml"), realColumns),
		fmt.Sprintf("INSERT INTO %s %s", q("dml"), realRows(1, 1001)),
		"DELETE FROM "+q("dml")+" WHERE id % 10 = 0",
		"UPDATE "+q("dml")+" SET s = 'upd-'||id, g = g + 100 WHERE id % 10 = 1",
		fmt.Sprintf("INSERT INTO %s %s", q("dml"), realRows(1001, 1101)),
		"DELETE FROM "+q("dml")+" WHERE id % 10 = 2",

		fmt.Sprintf("CREATE TABLE %s (%s) PARTITIONED BY (g)", q("part"), realColumns),
		fmt.Sprintf("INSERT INTO %s %s", q("part"), realRows(1, 501)),
		fmt.Sprintf("INSERT INTO %s %s", q("part"), realRows(501, 601)),

		fmt.Sprintf("CREATE TABLE %s (%s) PARTITIONED BY (month(d), bucket(4, id))", q("part_xform"), realColumns),
		fmt.Sprintf("INSERT INTO %s %s", q("part_xform"), realRows(1, 301)),

		fmt.Sprintf("CREATE TABLE %s (%s) PARTITIONED BY (d)", q("part_del"), realColumns),
		fmt.Sprintf("INSERT INTO %s %s", q("part_del"), realRows(1, 501)),
		"DELETE FROM "+q("part_del")+" WHERE id % 10 = 0",
		"UPDATE "+q("part_del")+" SET s = 'u' WHERE id % 10 = 1",
	)
	return map[string]int64{
		"empty": 0, "inserts": 1100, "deletes": 900, "dml": 890,
		"part": 600, "part_xform": 300, "part_del": 450,
	}
}

// tableState is everything about a table that a rebase must not change.
type tableState struct {
	rows       [][]string
	snapshots  map[int64]int64 // snapshot id -> sequence number
	current    *int64
	nextRowID  *int64
	timeTravel map[int64]int64 // snapshot id -> row count, through DuckDB
	py         *pyRead
}

type pyRead struct {
	Count     int64            `json:"count"`
	Digest    string           `json:"digest"`
	Snapshots map[string]int64 `json:"snapshots"`
}

func captureState(ctx context.Context, t *testing.T, s *harness.Stack, reader harness.DuckDB, py *harness.PyIceberg, ns, table string) tableState {
	t.Helper()
	cat := s.LoadCatalogTable(ctx, t, ns, table)
	st := tableState{
		snapshots:  map[int64]int64{},
		current:    cat.CurrentSnapshot,
		nextRowID:  cat.NextRowID,
		timeTravel: map[int64]int64{},
	}
	for _, sn := range cat.Snapshots {
		st.snapshots[sn.ID] = sn.SequenceNumber
	}
	if py != nil {
		st.py = &pyRead{}
		py.Call(t, s, ns, "read", map[string]any{"table": table}, st.py)
	}
	if reader.Bin == "" { // pyiceberg only; see noDuckDBReader.
		return st
	}
	st.rows = reader.Query(t, s, fmt.Sprintf("SELECT * FROM lk.%s.%s ORDER BY ALL", ns, table))
	var perSnapshot []string
	for _, sn := range cat.Snapshots {
		perSnapshot = append(perSnapshot, fmt.Sprintf("SELECT %d, count(*) FROM lk.%s.%s AT (VERSION => %d)", sn.ID, ns, table, sn.ID))
	}
	if len(perSnapshot) > 0 {
		for _, row := range reader.Query(t, s, strings.Join(perSnapshot, " UNION ALL ")) {
			id, err := strconv.ParseInt(row[0], 10, 64)
			if err != nil {
				t.Fatalf("%s: bad snapshot id %v: %v", table, row, err)
			}
			n, err := strconv.ParseInt(row[1], 10, 64)
			if err != nil {
				t.Fatalf("%s: bad count %v: %v", table, row, err)
			}
			st.timeTravel[id] = n
		}
	}
	return st
}

// noDuckDBReader leaves DuckDB out of a state comparison, for tables it cannot
// read before any rebase: DuckDB 1.5.6 fails on V1 manifests carried into a V2
// list ("required field 'sequence_number' is NULL").
var noDuckDBReader = harness.DuckDB{}

func compareState(t *testing.T, table string, pre, post tableState) {
	t.Helper()
	if !reflect.DeepEqual(pre.rows, post.rows) {
		t.Errorf("%s: table contents changed: %d rows before, %d after", table, len(pre.rows), len(post.rows))
	}
	if !maps.Equal(pre.snapshots, post.snapshots) {
		t.Errorf("%s: snapshot ids or sequence numbers changed:\n pre  %v\n post %v", table, pre.snapshots, post.snapshots)
	}
	if !reflect.DeepEqual(pre.current, post.current) {
		t.Errorf("%s: current snapshot changed: %v -> %v", table, deref(pre.current), deref(post.current))
	}
	if !reflect.DeepEqual(pre.nextRowID, post.nextRowID) {
		t.Errorf("%s: next-row-id changed: %v -> %v", table, deref(pre.nextRowID), deref(post.nextRowID))
	}
	if !maps.Equal(pre.timeTravel, post.timeTravel) {
		t.Errorf("%s: time travel row counts changed:\n pre  %v\n post %v", table, pre.timeTravel, post.timeTravel)
	}
	if pre.py != nil && !reflect.DeepEqual(*pre.py, *post.py) {
		t.Errorf("%s: pyiceberg read changed:\n pre  %+v\n post %+v", table, *pre.py, *post.py)
	}
}

func deref(p *int64) any {
	if p == nil {
		return nil
	}
	return *p
}

// namespacePrefix returns the key prefix of a table's namespace directory,
// e.g. "<namespace-uuid>/" for s3://<bucket>/<namespace-uuid>/<table-uuid>.
func namespacePrefix(t *testing.T, bucket, location string) string {
	t.Helper()
	rest, ok := strings.CutPrefix(location, "s3://"+bucket+"/")
	if !ok {
		t.Fatalf("table location %q is not in bucket %q", location, bucket)
	}
	nsDir, _, ok := strings.Cut(rest, "/")
	if !ok {
		t.Fatalf("unexpected table location %q", location)
	}
	return nsDir + "/"
}

// rebaseAndVerify is the whole path a customer's table takes: capture what it
// holds, copy the bytes, run bergrb, delete the originals, and require that
// every reader still sees exactly the same thing. It returns the post states.
func rebaseAndVerify(ctx context.Context, t *testing.T, s *harness.Stack, reader harness.DuckDB, py *harness.PyIceberg, ns string, want map[string]int64) map[string]tableState {
	t.Helper()
	bucket := s.MinIO.TargetBucket
	tables := slices.Sorted(maps.Keys(want))
	pre := map[string]tableState{}
	var srcKey string
	for _, table := range tables {
		pre[table] = captureState(ctx, t, s, reader, py, ns, table)
		cat := s.LoadCatalogTable(ctx, t, ns, table)
		if !strings.HasSuffix(cat.MetadataLocation, ".gz.metadata.json") {
			t.Fatalf("%s: fixture is meant to have Lakekeeper's default gzip metadata, got %s", table, cat.MetadataLocation)
		}
		srcKey = namespacePrefix(t, bucket, cat.Location)
	}
	if n := s.MinIO.CopyPrefix(ctx, t, bucket, srcKey, bucket, "rebased/"+srcKey); n == 0 {
		t.Fatalf("no objects under %s", srcKey)
	}
	srcPrefix := "s3://" + bucket + "/" + srcKey
	dstPrefix := "s3://" + bucket + "/rebased/" + srcKey

	run := s.RunBergrb(t, ns, srcPrefix, dstPrefix, "--keep-going")
	if run.ExitCode != 0 {
		t.Fatalf("bergrb exit %d:\n%s", run.ExitCode, tail(run.Log))
	}
	s.MinIO.DeletePrefix(ctx, t, bucket, srcKey)

	post := map[string]tableState{}
	for _, table := range tables {
		cat := s.LoadCatalogTable(ctx, t, ns, table)
		if !strings.HasPrefix(cat.MetadataLocation, dstPrefix) || !strings.HasPrefix(cat.Location, dstPrefix) {
			t.Errorf("%s: not on the target prefix: %s", table, cat.MetadataLocation)
		}
		post[table] = captureState(ctx, t, s, reader, py, ns, table)
		compareState(t, table, pre[table], post[table])
		got := int64(len(post[table].rows))
		if reader.Bin == "" {
			got = post[table].py.Count
		}
		if got != want[table] {
			t.Errorf("%s: %d rows after rebase, want %d", table, got, want[table])
		}
	}

	again := s.RunBergrb(t, ns, srcPrefix, dstPrefix)
	if again.ExitCode != 0 {
		t.Errorf("second bergrb run exit %d, want a no-op:\n%s", again.ExitCode, tail(again.Log))
	}
	if n := strings.Count(again.Log, "table already migrated"); n != len(tables) {
		t.Errorf("second bergrb run reported %d tables already migrated, want %d", n, len(tables))
	}
	return post
}

func tail(log string) string {
	const limit = 4000
	if len(log) > limit {
		return "..." + log[len(log)-limit:]
	}
	return log
}

// TestRealWriters_DuckDB covers tables written by every DuckDB version in the
// matrix: gzipped metadata, manifest lists with no header keys, 63-bit snapshot
// IDs, position deletes, and on 1.5.4 the mislabelled delete manifests.
func TestRealWriters_DuckDB(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping testcontainers test in short mode")
	}
	writers := harness.DuckDBWriters(t)
	reader := harness.ReaderDuckDB(t)
	py := optionalPyIceberg(t)
	ctx := context.Background()
	s := harness.StartStack(ctx, t)
	for _, d := range writers {
		t.Run("duckdb-"+d.Label(), func(t *testing.T) {
			ns := "d" + strings.ReplaceAll(d.Label(), ".", "_")
			want := duckDBShapes(t, d, s, ns)
			post := rebaseAndVerify(ctx, t, s, reader, py, ns, want)

			// The rows a DELETE removed must still be gone.
			for _, table := range []string{"deletes", "dml", "part_del"} {
				if len(post[table].rows) == 0 {
					t.Fatalf("%s: no rows", table)
				}
				for _, row := range post[table].rows {
					id, err := strconv.ParseInt(row[0], 10, 64)
					if err != nil {
						t.Fatalf("%s: bad id in %v: %v", table, row, err)
					}
					if id <= 1000 && id%10 == 0 {
						t.Fatalf("%s: deleted row %v came back", table, row)
					}
				}
			}

			// The moved table stays writable, on the new prefix.
			d.Exec(t, s, fmt.Sprintf("INSERT INTO lk.%s.deletes %s", ns, realRows(900001, 900011)))
			if got := reader.QueryInt64(t, s, fmt.Sprintf("SELECT count(*) FROM lk.%s.deletes", ns)); got != want["deletes"]+10 {
				t.Errorf("after post-rebase insert: %d rows, want %d", got, want["deletes"]+10)
			}
			if loc := s.LoadCatalogTable(ctx, t, ns, "deletes").MetadataLocation; !strings.Contains(loc, "/rebased/") {
				t.Errorf("post-rebase commit left the target prefix: %s", loc)
			}
		})
	}
}

// optionalPyIceberg adds pyiceberg as a second reader when it is available. It
// is required in CI, where NewPyIceberg fails rather than skips.
func optionalPyIceberg(t *testing.T) *harness.PyIceberg {
	t.Helper()
	if os.Getenv("BERGREBASE_TEST_PYTHON") == "" && !harness.Required() {
		return nil
	}
	py := harness.NewPyIceberg(t)
	return &py
}

// TestRealWriters_PyIceberg covers pyiceberg-written tables: manifests with no
// schema-id key, and copy-on-write deletes, which leave DELETED manifest
// entries. Those entries are the regression from c620f78: rewriting them as
// ADDED brought the deleted rows back, with exit code 0.
func TestRealWriters_PyIceberg(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping testcontainers test in short mode")
	}
	py := harness.NewPyIceberg(t)
	reader := harness.ReaderDuckDB(t)
	ctx := context.Background()
	s := harness.StartStack(ctx, t)
	const ns = "py"
	py.Call(t, s, ns, "write_shapes", nil, nil)
	want := map[string]int64{
		"empty": 0, "inserts": 1100, "deletes": 900, "replace": 300, "upsert": 600,
		"part": 600, "part_xform": 200, "part_del": 400,
	}
	post := rebaseAndVerify(ctx, t, s, reader, &py, ns, want)

	// Exact contents, not only equality with the pre-rebase read: ids 100..199
	// are gone from the tables whose last commit deleted them.
	for _, table := range []string{"deletes", "part_del"} {
		ids := map[int64]bool{}
		for _, row := range post[table].rows {
			id, err := strconv.ParseInt(row[0], 10, 64)
			if err != nil {
				t.Fatalf("%s: bad id in %v: %v", table, row, err)
			}
			if ids[id] {
				t.Errorf("%s: id %d appears twice", table, id)
			}
			ids[id] = true
			if id >= 100 && id < 200 {
				t.Errorf("%s: deleted row %d came back", table, id)
			}
		}
		if !ids[99] || !ids[200] {
			t.Errorf("%s: surviving rows around the deleted range are missing", table)
		}
	}
	// History that has a DELETED entry in it: the truncate snapshot of a replace
	// held no rows and must still hold none.
	sawEmptySnapshot := false
	for _, n := range post["replace"].timeTravel {
		sawEmptySnapshot = sawEmptySnapshot || n == 0
	}
	if !sawEmptySnapshot {
		t.Errorf("replace: the truncate snapshot has rows again: %v", post["replace"].timeTravel)
	}
}

// TestRealWriters_CompactedByPyIceberg is a DuckDB table after a pyiceberg
// compaction, the state of a table on a box after iceberg-maintenance has run.
// The current snapshot is fine even with a status bug; the snapshot holding the
// DELETED entries is what time travel would get wrong.
func TestRealWriters_CompactedByPyIceberg(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping testcontainers test in short mode")
	}
	py := harness.NewPyIceberg(t)
	reader := harness.ReaderDuckDB(t)
	ctx := context.Background()
	s := harness.StartStack(ctx, t)
	const ns = "mixed"
	stmts := []string{"CREATE SCHEMA lk." + ns, fmt.Sprintf("CREATE TABLE lk.%s.compacted (%s)", ns, realColumns)}
	for k := range 10 {
		stmts = append(stmts, fmt.Sprintf("INSERT INTO lk.%s.compacted %s", ns, realRows(1+100*k, 101+100*k)))
	}
	reader.Exec(t, s, stmts...)
	py.Call(t, s, ns, "compact", map[string]any{"table": "compacted"}, nil)
	post := rebaseAndVerify(ctx, t, s, reader, &py, ns, map[string]int64{"compacted": 1000})
	if len(post["compacted"].timeTravel) < 11 {
		t.Errorf("expected the compaction's snapshots in history, got %v", post["compacted"].timeTravel)
	}
}

// TestRealWriters_DuckDBContentHeaderBug pins that the matrix really contains a
// writer with the content=data delete-manifest bug, that the fixture exhibits
// it, and that the rewritten manifest is labelled correctly.
func TestRealWriters_DuckDBContentHeaderBug(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping testcontainers test in short mode")
	}
	var buggy *harness.DuckDB
	for _, d := range harness.DuckDBWriters(t) {
		if d.HasContentHeaderBug() {
			buggy = &d
			break
		}
	}
	if buggy == nil {
		harness.SkipOrFail(t, "no DuckDB <= 1.5.4 in BERGREBASE_TEST_DUCKDB_WRITERS")
		return
	}
	reader := harness.ReaderDuckDB(t)
	ctx := context.Background()
	s := harness.StartStack(ctx, t)
	store := s.StorageClient()
	const ns = "hdr"
	buggy.Exec(t, s,
		"CREATE SCHEMA lk."+ns,
		fmt.Sprintf("CREATE TABLE lk.%s.t (%s)", ns, realColumns),
		fmt.Sprintf("INSERT INTO lk.%s.t %s", ns, realRows(1, 1001)),
		fmt.Sprintf("DELETE FROM lk.%s.t WHERE id %% 10 = 0", ns),
	)
	if got := deleteManifestLabels(ctx, t, s, store, ns, "t"); !slices.Equal(got, []string{"data"}) {
		t.Fatalf("DuckDB %s no longer mislabels delete manifests (labels %v); this fixture needs a writer that does", buggy.Version, got)
	}
	post := rebaseAndVerify(ctx, t, s, reader, nil, ns, map[string]int64{"t": 900})
	if len(post["t"].rows) != 900 {
		t.Errorf("%d rows after rebase, want 900", len(post["t"].rows))
	}
	if got := deleteManifestLabels(ctx, t, s, store, ns, "t"); !slices.Equal(got, []string{"deletes"}) {
		t.Errorf("rewritten delete manifests are labelled %v, want deletes", got)
	}
}

// deleteManifestLabels returns the OCF content header of each delete manifest
// in the table's current snapshot.
func deleteManifestLabels(ctx context.Context, t *testing.T, s *harness.Stack, store *bergstorage.Client, ns, table string) []string {
	t.Helper()
	var labels []string
	for _, mf := range currentManifests(ctx, t, s, store, ns, table) {
		if mf.ManifestContent() != iceberg.ManifestContentDeletes {
			continue
		}
		raw, err := store.GetObject(ctx, mf.FilePath())
		if err != nil {
			t.Fatalf("get manifest: %v", err)
		}
		labels = append(labels, string(harness.OCFMetadata(t, raw)["content"]))
	}
	return labels
}

func currentManifests(ctx context.Context, t *testing.T, s *harness.Stack, store *bergstorage.Client, ns, table string) []iceberg.ManifestFile {
	t.Helper()
	cat := s.LoadCatalogTable(ctx, t, ns, table)
	for _, sn := range cat.Snapshots {
		if cat.CurrentSnapshot == nil || sn.ID != *cat.CurrentSnapshot {
			continue
		}
		raw, err := store.GetObject(ctx, sn.ManifestList)
		if err != nil {
			t.Fatalf("get manifest list: %v", err)
		}
		files, err := iceberg.ReadManifestList(bytes.NewReader(raw))
		if err != nil {
			t.Fatalf("decode manifest list: %v", err)
		}
		return files
	}
	t.Fatalf("%s.%s has no current snapshot", ns, table)
	return nil
}

// TestRealWriters_V3RowLineage rebases a multi-snapshot V3 table and requires
// every manifest's first_row_id to survive. Plain queries cannot see a wrong
// value, so the assertion reads the manifest lists. Deletion vectors stay
// refused, and leave the table untouched.
func TestRealWriters_V3RowLineage(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping testcontainers test in short mode")
	}
	reader := harness.ReaderDuckDB(t)
	ctx := context.Background()
	s := harness.StartStack(ctx, t)
	store := s.StorageClient()
	const ns = "v3"
	v3 := " WITH ('format-version'='3')"
	reader.Exec(t, s,
		"CREATE SCHEMA lk."+ns,
		fmt.Sprintf("CREATE TABLE lk.%s.lineage (%s)%s", ns, realColumns, v3),
		fmt.Sprintf("INSERT INTO lk.%s.lineage %s", ns, realRows(1, 1001)),
		fmt.Sprintf("INSERT INTO lk.%s.lineage %s", ns, realRows(1001, 1101)),
		fmt.Sprintf("INSERT INTO lk.%s.lineage %s", ns, realRows(1101, 1151)),
	)
	before := rowIDRanges(ctx, t, s, store, ns, "lineage")
	if len(before) != 3 {
		t.Fatalf("expected 3 data manifests, got %v", before)
	}
	rebaseAndVerify(ctx, t, s, reader, nil, ns, map[string]int64{"lineage": 1150})
	after := rowIDRanges(ctx, t, s, store, ns, "lineage")
	if !slices.Equal(before, after) {
		t.Errorf("manifest first_row_id changed: %v -> %v", before, after)
	}

	// A second namespace, so the refusal is not mixed up with the move above.
	const refused = "v3dv"
	reader.Exec(t, s,
		"CREATE SCHEMA lk."+refused,
		fmt.Sprintf("CREATE TABLE lk.%s.dv (%s)%s", refused, realColumns, v3),
		fmt.Sprintf("INSERT INTO lk.%s.dv %s", refused, realRows(1, 101)),
		fmt.Sprintf("DELETE FROM lk.%s.dv WHERE id = 1", refused),
	)
	cat := s.LoadCatalogTable(ctx, t, refused, "dv")
	bucket := s.MinIO.TargetBucket
	key := namespacePrefix(t, bucket, cat.Location)
	s.MinIO.CopyPrefix(ctx, t, bucket, key, bucket, "rebased/"+key)
	run := s.RunBergrb(t, refused, "s3://"+bucket+"/"+key, "s3://"+bucket+"/rebased/"+key)
	if run.ExitCode == 0 || !strings.Contains(run.Log, "V3 deletion vector") {
		t.Errorf("V3 deletion vector not refused: exit %d\n%s", run.ExitCode, tail(run.Log))
	}
	if got := s.LoadCatalogTable(ctx, t, refused, "dv").MetadataLocation; got != cat.MetadataLocation {
		t.Errorf("refused table was moved: %s -> %s", cat.MetadataLocation, got)
	}
}

func rowIDRanges(ctx context.Context, t *testing.T, s *harness.Stack, store *bergstorage.Client, ns, table string) []int64 {
	t.Helper()
	var ids []int64
	for _, mf := range currentManifests(ctx, t, s, store, ns, table) {
		if mf.ManifestContent() != iceberg.ManifestContentData {
			continue
		}
		if mf.FirstRowID() == nil {
			t.Fatalf("data manifest %s has no first_row_id", mf.FilePath())
		}
		ids = append(ids, *mf.FirstRowID())
	}
	slices.Sort(ids)
	return ids
}

// TestRealWriters_BucketMove runs the same-catalog bucket move from
// docs/migration-recipes.md. Lakekeeper will not change a warehouse's bucket
// over its API and will not register a table outside the warehouse's bucket, so
// the warehouse row has to be edited first.
func TestRealWriters_BucketMove(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping testcontainers test in short mode")
	}
	reader := harness.ReaderDuckDB(t)
	ctx := context.Background()
	s := harness.StartStack(ctx, t)
	const ns = "move"
	want := duckDBShapes(t, reader, s, ns)
	from, to := s.MinIO.TargetBucket, s.MinIO.SourceBucket // The stack's warehouse starts on "target".

	pre := map[string]tableState{}
	var key string
	for table := range want {
		pre[table] = captureState(ctx, t, s, reader, nil, ns, table)
		key = namespacePrefix(t, from, s.LoadCatalogTable(ctx, t, ns, table).Location)
	}
	s.MinIO.CopyPrefix(ctx, t, from, key, to, key)
	srcPrefix, dstPrefix := "s3://"+from+"/"+key, "s3://"+to+"/"+key

	// Without the row edit: the catalog refuses, bergrb rolls back, tables stay.
	refused := s.RunBergrb(t, ns, srcPrefix, dstPrefix, "--table", "inserts")
	if refused.ExitCode == 0 {
		t.Fatalf("register outside the warehouse bucket succeeded; the recipe's ordering is out of date:\n%s", tail(refused.Log))
	}
	if loc := s.LoadCatalogTable(ctx, t, ns, "inserts").MetadataLocation; !strings.HasPrefix(loc, srcPrefix) {
		t.Fatalf("refused swap moved the table: %s", loc)
	}
	if code, body := s.UpdateWarehouseBucketViaAPI(ctx, t, to); code != http.StatusBadRequest || !strings.Contains(body, "bucket") {
		t.Fatalf("management API bucket update: %d %s; the recipe says this is refused", code, body)
	}

	s.SetWarehouseBucketInPostgres(ctx, t, to)
	run := s.RunBergrb(t, ns, srcPrefix, dstPrefix)
	if run.ExitCode != 0 {
		t.Fatalf("bergrb after the row edit: exit %d\n%s", run.ExitCode, tail(run.Log))
	}
	s.MinIO.DeletePrefix(ctx, t, from, key)
	for table, n := range want {
		post := captureState(ctx, t, s, reader, nil, ns, table)
		compareState(t, table, pre[table], post)
		if int64(len(post.rows)) != n {
			t.Errorf("%s: %d rows, want %d", table, len(post.rows), n)
		}
	}
	reader.Exec(t, s, fmt.Sprintf("INSERT INTO lk.%s.inserts %s", ns, realRows(900001, 900002)))
	loc := s.LoadCatalogTable(ctx, t, ns, "inserts").MetadataLocation
	if !strings.HasPrefix(loc, "s3://"+to+"/") {
		t.Errorf("post-move commit is not in the new bucket: %s", loc)
	}
	if len(s.MinIO.ListKeys(ctx, t, from, key)) != 0 {
		t.Errorf("post-move writes landed in the old bucket")
	}
}

// TestRealWriters_Scale moves a table with many snapshots, manifests and data
// files from each writer. BERGREBASE_TEST_SCALE multiplies the batch size and
// count, for a one-off larger run.
func TestRealWriters_Scale(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping testcontainers test in short mode")
	}
	harness.MaybeSkipNotNightly(t)
	py := harness.NewPyIceberg(t)
	reader := harness.ReaderDuckDB(t)
	scale := 1
	if v := os.Getenv("BERGREBASE_TEST_SCALE"); v != "" {
		var err error
		if scale, err = strconv.Atoi(v); err != nil || scale < 1 {
			t.Fatalf("BERGREBASE_TEST_SCALE=%q", v)
		}
	}
	batches, batchRows := 30*scale, 5000*scale
	total := int64(batches * batchRows)
	ctx := context.Background()
	s := harness.StartStack(ctx, t)
	store := s.StorageClient()

	const ns = "scale"
	stmts := []string{"CREATE SCHEMA lk." + ns, fmt.Sprintf("CREATE TABLE lk.%s.duck (%s) PARTITIONED BY (g)", ns, realColumns)}
	for k := range batches {
		stmts = append(stmts, fmt.Sprintf("INSERT INTO lk.%s.duck %s", ns, realRows(1+k*batchRows, 1+(k+1)*batchRows)))
	}
	stmts = append(stmts,
		fmt.Sprintf("DELETE FROM lk.%s.duck WHERE id %% 100 = 0", ns),
		fmt.Sprintf("UPDATE lk.%s.duck SET s = 'upd' WHERE id %% 100 = 1", ns),
	)
	reader.Exec(t, s, stmts...)
	py.Call(t, s, ns, "append_batches", map[string]any{"table": "py", "batches": batches, "batch_rows": batchRows}, nil)

	want := map[string]int64{"duck": total - total/100, "py": total - 100}
	for table := range want {
		manifests := currentManifests(ctx, t, s, store, ns, table)
		var files int32
		for _, mf := range manifests {
			files += mf.AddedDataFiles() + mf.ExistingDataFiles()
		}
		snaps := len(s.LoadCatalogTable(ctx, t, ns, table).Snapshots)
		t.Logf("%s: %d rows, %d snapshots, %d manifests and %d data files in the current snapshot", table, want[table], snaps, len(manifests), files)
		if len(manifests) < 10 || files < 100 || snaps < batches {
			t.Errorf("%s: not at scale: %d manifests, %d files, %d snapshots", table, len(manifests), files, snaps)
		}
	}
	rebaseAndVerify(ctx, t, s, reader, nil, ns, want)
}

// assertManifestLengths requires manifest_length to equal the object's size in
// every snapshot's list.
func assertManifestLengths(ctx context.Context, t *testing.T, s *harness.Stack, store *bergstorage.Client, ns, table string) {
	t.Helper()
	for _, sn := range s.LoadCatalogTable(ctx, t, ns, table).Snapshots {
		raw, err := store.GetObject(ctx, sn.ManifestList)
		if err != nil {
			t.Fatalf("get list: %v", err)
		}
		files, err := iceberg.ReadManifestList(bytes.NewReader(raw))
		if err != nil {
			t.Fatalf("decode list %s: %v", sn.ManifestList, err)
		}
		for _, mf := range files {
			size, err := store.HeadObject(ctx, mf.FilePath())
			if err != nil {
				t.Fatalf("head %s: %v", mf.FilePath(), err)
			}
			if mf.Length() != size {
				t.Errorf("%s: snapshot %d says manifest_length %d for %s, object is %d", table, sn.ID, mf.Length(), mf.FilePath(), size)
			}
		}
	}
}

// TestRealWriters_UpgradedTable moves a table upgraded from format version 1 to
// 2, whose old manifests are referenced by lists of both versions, and then
// moves it again. Each manifest has to stay in its own format, written once.
func TestRealWriters_UpgradedTable(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping testcontainers test in short mode")
	}
	py := harness.NewPyIceberg(t)
	ctx := context.Background()
	s := harness.StartStack(ctx, t)
	store := s.StorageClient()
	const ns, table = "up", "upgraded"
	py.Call(t, s, ns, "v1_upgrade", map[string]any{"table": table}, nil)
	want := map[string]int64{table: 151}
	first := rebaseAndVerify(ctx, t, s, noDuckDBReader, &py, ns, want)[table]
	assertManifestLengths(ctx, t, s, store, ns, table)

	bucket := s.MinIO.TargetBucket
	key := "rebased/" + namespacePrefix(t, bucket, strings.Replace(s.LoadCatalogTable(ctx, t, ns, table).Location, "/rebased/", "/", 1))
	nextKey := "again/" + strings.TrimPrefix(key, "rebased/")
	s.MinIO.CopyPrefix(ctx, t, bucket, key, bucket, nextKey)
	run := s.RunBergrb(t, ns, "s3://"+bucket+"/"+key, "s3://"+bucket+"/"+nextKey)
	if run.ExitCode != 0 {
		t.Fatalf("second migration: exit %d\n%s", run.ExitCode, tail(run.Log))
	}
	s.MinIO.DeletePrefix(ctx, t, bucket, key)
	compareState(t, table, first, captureState(ctx, t, s, noDuckDBReader, &py, ns, table))
	assertManifestLengths(ctx, t, s, store, ns, table)
}

// TestRealWriters_IncompleteCopy is an operator's incomplete byte copy. The
// first run swaps and fails validation. A re-run must keep failing rather than
// call the table done; then both documented ways out must work.
func TestRealWriters_IncompleteCopy(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping testcontainers test in short mode")
	}
	reader := harness.ReaderDuckDB(t)
	ctx := context.Background()
	s := harness.StartStack(ctx, t)
	const ns = "partial"
	reader.Exec(t, s, "CREATE SCHEMA lk."+ns,
		fmt.Sprintf("CREATE TABLE lk.%s.t (%s)", ns, realColumns),
		fmt.Sprintf("INSERT INTO lk.%s.t %s", ns, realRows(1, 101)))
	before := s.LoadCatalogTable(ctx, t, ns, "t")
	pre := captureState(ctx, t, s, reader, nil, ns, "t")
	bucket := s.MinIO.TargetBucket
	key := namespacePrefix(t, bucket, before.Location)
	src, dst := "s3://"+bucket+"/"+key, "s3://"+bucket+"/rebased/"+key

	copyWithoutData := func() {
		t.Helper()
		s.MinIO.CopyPrefix(ctx, t, bucket, key, bucket, "rebased/"+key)
		for _, k := range s.MinIO.ListKeys(ctx, t, bucket, "rebased/"+key) {
			if strings.HasSuffix(k, ".parquet") {
				s.MinIO.DeletePrefix(ctx, t, bucket, k)
			}
		}
	}
	copyWithoutData()

	first := s.RunBergrb(t, ns, src, dst)
	if first.ExitCode == 0 || !strings.Contains(first.Log, "post-swap validation") {
		t.Fatalf("first run with data files missing: exit %d\n%s", first.ExitCode, tail(first.Log))
	}
	if loc := s.LoadCatalogTable(ctx, t, ns, "t").MetadataLocation; !strings.HasPrefix(loc, dst) {
		t.Fatalf("fixture assumes the first run swapped, pointer is %s", loc)
	}
	second := s.RunBergrb(t, ns, src, dst)
	if second.ExitCode == 0 {
		t.Fatalf("re-run reported success with data files missing:\n%s", tail(second.Log))
	}
	if !strings.Contains(second.Log, "already-migrated table failed validation") {
		t.Errorf("re-run does not say what is wrong:\n%s", tail(second.Log))
	}
	if run := s.RunBergrb(t, ns, src, dst, "--no-validate"); run.ExitCode != 0 {
		t.Errorf("--no-validate on an already-migrated table: exit %d", run.ExitCode)
	}

	// Way out 1: swap back to the source, which still has everything. The
	// overwrite goes in the body: Lakekeeper ignores it as a query parameter.
	s.SwapTableBack(ctx, t, ns, "t", before.MetadataLocation)
	compareState(t, "t", pre, captureState(ctx, t, s, reader, nil, ns, "t"))

	// Way out 2: finish the copy and run again. From the swapped state as well.
	copyWithoutData()
	if run := s.RunBergrb(t, ns, src, dst); run.ExitCode == 0 {
		t.Fatalf("second incomplete copy succeeded:\n%s", tail(run.Log))
	}
	// Only what is missing: a plain re-copy would overwrite the rewritten
	// metadata at the same keys with the originals.
	for _, k := range s.MinIO.ListKeys(ctx, t, bucket, key) {
		if strings.HasSuffix(k, ".parquet") {
			s.MinIO.CopyPrefix(ctx, t, bucket, k, bucket, "rebased/"+k)
		}
	}
	if run := s.RunBergrb(t, ns, src, dst); run.ExitCode != 0 {
		t.Fatalf("re-run after completing the copy: exit %d\n%s", run.ExitCode, tail(run.Log))
	}
	s.MinIO.DeletePrefix(ctx, t, bucket, key)
	compareState(t, "t", pre, captureState(ctx, t, s, reader, nil, ns, "t"))
}

// liveFiles returns the data and delete files the table's current snapshot
// references and does not mark DELETED.
func liveFiles(ctx context.Context, t *testing.T, s *harness.Stack, store *bergstorage.Client, ns, table string) (data, deletes []string) {
	t.Helper()
	for _, mf := range currentManifests(ctx, t, s, store, ns, table) {
		raw, err := store.GetObject(ctx, mf.FilePath())
		if err != nil {
			t.Fatalf("get manifest: %v", err)
		}
		entries, err := iceberg.ReadManifest(mf, bytes.NewReader(raw), false)
		if err != nil {
			t.Fatalf("decode manifest %s: %v", mf.FilePath(), err)
		}
		for _, e := range entries {
			if e.Status() == iceberg.EntryStatusDELETED {
				continue
			}
			if mf.ManifestContent() == iceberg.ManifestContentDeletes {
				deletes = append(deletes, e.DataFile().FilePath())
			} else {
				data = append(data, e.DataFile().FilePath())
			}
		}
	}
	return data, deletes
}

// TestRealWriters_ValidationSeesWholeCopy: validation must notice any one live
// file missing from the target, data or delete, not only the first; and a
// plain re-copy of the source after the rebase, which puts the original
// metadata back at the rewritten keys, must fail it too.
func TestRealWriters_ValidationSeesWholeCopy(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping testcontainers test in short mode")
	}
	reader := harness.ReaderDuckDB(t)
	ctx := context.Background()
	s := harness.StartStack(ctx, t)
	store := s.StorageClient()
	const ns = "whole"
	reader.Exec(t, s, "CREATE SCHEMA lk."+ns,
		fmt.Sprintf("CREATE TABLE lk.%s.t (%s)", ns, realColumns),
		fmt.Sprintf("INSERT INTO lk.%s.t %s", ns, realRows(1, 101)),
		fmt.Sprintf("INSERT INTO lk.%s.t %s", ns, realRows(101, 201)),
		fmt.Sprintf("INSERT INTO lk.%s.t %s", ns, realRows(201, 301)),
		fmt.Sprintf("DELETE FROM lk.%s.t WHERE id %% 10 = 0", ns))
	before := s.LoadCatalogTable(ctx, t, ns, "t")
	pre := captureState(ctx, t, s, reader, nil, ns, "t")
	data, deletes := liveFiles(ctx, t, s, store, ns, "t")
	if len(data) < 3 || len(deletes) == 0 {
		t.Fatalf("fixture wants >= 3 live data files and a delete file, got %d and %d", len(data), len(deletes))
	}
	bucket := s.MinIO.TargetBucket
	key := namespacePrefix(t, bucket, before.Location)
	src, dst := "s3://"+bucket+"/"+key, "s3://"+bucket+"/rebased/"+key
	targetKey := func(uri string) string {
		return "rebased/" + strings.TrimPrefix(uri, "s3://"+bucket+"/")
	}
	s.MinIO.CopyPrefix(ctx, t, bucket, key, bucket, "rebased/"+key)

	// One file missing at a time: the last data file, then a delete file.
	for i, missing := range []string{data[len(data)-1], deletes[0]} {
		s.MinIO.DeletePrefix(ctx, t, bucket, targetKey(missing))
		run := s.RunBergrb(t, ns, src, dst)
		if run.ExitCode == 0 {
			t.Fatalf("case %d: bergrb exited 0 with %s missing from the target:\n%s", i, missing, tail(run.Log))
		}
		if !strings.Contains(run.Log, strings.TrimPrefix(targetKey(missing), "rebased/")) {
			t.Errorf("case %d: log does not name the missing file %s:\n%s", i, missing, tail(run.Log))
		}
		s.MinIO.CopyPrefix(ctx, t, bucket, strings.TrimPrefix(missing, "s3://"+bucket+"/"), bucket, targetKey(missing))
		if run := s.RunBergrb(t, ns, src, dst); run.ExitCode != 0 {
			t.Fatalf("case %d: re-run with the copy complete: exit %d\n%s", i, run.ExitCode, tail(run.Log))
		}
	}

	// A plain re-copy after the rebase: every file is present, but the
	// metadata at the target names the source again.
	s.MinIO.CopyPrefix(ctx, t, bucket, key, bucket, "rebased/"+key)
	run := s.RunBergrb(t, ns, src, dst)
	if run.ExitCode == 0 {
		t.Fatalf("bergrb exited 0 after the source was re-copied over the rewritten metadata:\n%s", tail(run.Log))
	}
	if !strings.Contains(run.Log, "source prefix") {
		t.Errorf("log does not say metadata points at the source prefix:\n%s", tail(run.Log))
	}
	// The documented way out: swap back to the source and run bergrb again,
	// which rewrites the metadata at the target.
	s.SwapTableBack(ctx, t, ns, "t", before.MetadataLocation)
	if run := s.RunBergrb(t, ns, src, dst); run.ExitCode != 0 {
		t.Fatalf("re-run after swapping back: exit %d\n%s", run.ExitCode, tail(run.Log))
	}
	s.MinIO.DeletePrefix(ctx, t, bucket, key)
	compareState(t, "t", pre, captureState(ctx, t, s, reader, nil, ns, "t"))
}
