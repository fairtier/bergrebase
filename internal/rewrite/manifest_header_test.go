package rewrite

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	iceberg "github.com/apache/iceberg-go"
)

// manifestListCase is one manifest list shape the header reader must
// round-trip: every combination the writer side (RewriteManifestList →
// iceberg.WriteManifestList) can emit.
type manifestListCase struct {
	name       string
	version    int
	snapshotID int64
	parent     *int64
	seq        *int64
	firstRowID int64
}

func int64p(v int64) *int64 { return &v }

func manifestListCases() []manifestListCase {
	return []manifestListCase{
		{name: "v1 no parent", version: 1, snapshotID: 1},
		{name: "v1 parent", version: 1, snapshotID: 2, parent: int64p(1)},
		{name: "v1 max ids", version: 1, snapshotID: math.MaxInt64, parent: int64p(math.MaxInt64 - 1)},
		{name: "v2 no parent", version: 2, snapshotID: 7, seq: int64p(1)},
		{name: "v2 parent", version: 2, snapshotID: 8, parent: int64p(7), seq: int64p(2)},
		{name: "v2 parent zero", version: 2, snapshotID: 9, parent: int64p(0), seq: int64p(0)},
		{name: "v2 max", version: 2, snapshotID: math.MaxInt64, parent: int64p(math.MaxInt64 - 1), seq: int64p(math.MaxInt64)},
		{name: "v3 first row zero", version: 3, snapshotID: 11, seq: int64p(1)},
		{name: "v3 first row set", version: 3, snapshotID: 12, parent: int64p(11), seq: int64p(2), firstRowID: 40},
	}
}

// buildManifestList writes a manifest list for c over two manifests via
// iceberg.WriteManifestList — the same writer the test harness and
// RewriteManifestList use.
func buildManifestList(t *testing.T, c manifestListCase) ([]byte, []iceberg.ManifestFile) {
	t.Helper()
	files := make([]iceberg.ManifestFile, 0, 2)
	for i, p := range []string{"s3://old/t/metadata/m1.avro", "s3://old/t/metadata/m2.avro"} {
		b := iceberg.NewManifestFile(c.version, p, int64(1000+i), 0, c.snapshotID).
			AddedFiles(int32(i + 1)).
			AddedRows(int64(10 * (i + 1)))
		if c.version >= 2 {
			s := *c.seq
			b = b.SequenceNum(s, s)
		}
		files = append(files, b.Build())
	}
	buf := &bytes.Buffer{}
	if err := iceberg.WriteManifestList(c.version, buf, c.snapshotID, c.parent, c.seq, c.firstRowID, files); err != nil {
		t.Fatalf("WriteManifestList: %v", err)
	}
	return buf.Bytes(), files
}

func wantHeader(c manifestListCase) ManifestListHeader {
	h := ManifestListHeader{SnapshotID: c.snapshotID, ParentSnapshotID: c.parent}
	if c.version >= 2 {
		h.SequenceNumber = c.seq
	}
	if c.version >= 3 {
		h.FirstRowID = c.firstRowID
	}
	return h
}

// TestReadManifestListHeader_Versions asserts the exact header values
// for every V1/V2/V3 shape, including max-int64 ids and an explicit zero
// parent (which must stay distinct from "no parent").
func TestReadManifestListHeader_Versions(t *testing.T) {
	for _, c := range manifestListCases() {
		t.Run(c.name, func(t *testing.T) {
			raw, _ := buildManifestList(t, c)
			hdr, version, err := readManifestListHeader(raw)
			if err != nil {
				t.Fatalf("readManifestListHeader: %v", err)
			}
			if version != c.version {
				t.Errorf("version = %d, want %d", version, c.version)
			}
			if want := wantHeader(c); !reflect.DeepEqual(hdr, want) {
				t.Errorf("header = %s, want %s", fmtHeader(hdr), fmtHeader(want))
			}
		})
	}
}

// TestManifestList_RoundTripThroughRewrite reads each list through
// WalkManifestList, re-emits it with RewriteManifestList under a no-op
// mapping, and requires:
//   - the OCF header metadata (schema JSON, codec, every key) to be equal
//     as a map — iceberg-go writes the keys in Go map order, so the header
//     bytes legitimately differ in key order between two writes;
//   - everything after the header (every data block) to be equal byte for
//     byte, modulo the per-writer random sync marker;
//   - the decoded manifest files to be deep-equal.
func TestManifestList_RoundTripThroughRewrite(t *testing.T) {
	for _, c := range manifestListCases() {
		t.Run(c.name, func(t *testing.T) {
			raw, _ := buildManifestList(t, c)
			const listURI = "s3://old/t/metadata/snap.avro"
			store := newMemStorage()
			store.objects[listURI] = raw

			mapping := PrefixMapping{Source: "s3://nonmatching/", Target: "s3://other/"}
			_, files, hdr, version, err := WalkManifestList(context.Background(), store, listURI, mapping)
			if err != nil {
				t.Fatalf("WalkManifestList: %v", err)
			}
			if _, err := RewriteManifestList(context.Background(), store, listURI, mapping, version, hdr, files); err != nil {
				t.Fatalf("RewriteManifestList: %v", err)
			}
			out := store.puts[listURI]

			origMeta, err := readOCFMetadata(raw)
			if err != nil {
				t.Fatalf("readOCFMetadata(orig): %v", err)
			}
			outMeta, err := readOCFMetadata(out)
			if err != nil {
				t.Fatalf("readOCFMetadata(rewritten): %v", err)
			}
			if !reflect.DeepEqual(origMeta, outMeta) {
				t.Errorf("OCF metadata changed across the round trip:\norig: %v\nout:  %v", origMeta, outMeta)
			}
			origBody := normalizeSync(t, raw)[ocfHeaderLen(t, raw):]
			outBody := normalizeSync(t, out)[ocfHeaderLen(t, out):]
			if !bytes.Equal(origBody, outBody) {
				t.Errorf("data blocks differ across the round trip (%d vs %d bytes)", len(origBody), len(outBody))
			}
			reread, err := iceberg.ReadManifestList(bytes.NewReader(out))
			if err != nil {
				t.Fatalf("ReadManifestList(rewritten): %v", err)
			}
			if !reflect.DeepEqual(reread, files) {
				t.Errorf("decoded manifest files differ across the round trip")
			}
		})
	}
}

// normalizeSync replaces every occurrence of the file's sync marker (the
// trailing 16 bytes of any OCF) with zeroes.
func normalizeSync(t *testing.T, b []byte) []byte {
	t.Helper()
	if len(b) < 16 {
		t.Fatalf("OCF too short: %d bytes", len(b))
	}
	sync := b[len(b)-16:]
	return bytes.ReplaceAll(b, sync, make([]byte, 16))
}

// ocfHeaderLen returns the length of the OCF header in b: through the
// first occurrence of the sync marker (the file's trailing 16 bytes).
func ocfHeaderLen(t *testing.T, b []byte) int {
	t.Helper()
	i := bytes.Index(b[4:], b[len(b)-16:])
	if i < 0 {
		t.Fatalf("sync marker not found")
	}
	return 4 + i + 16
}

// TestReadOCFMetadata_ForeignWriter reads a manifest list written by
// DuckDB (copied from apache/iceberg-go's testdata, Apache-2.0). The
// OCF header must parse; DuckDB writes no format-version key, so the
// manifest-list header is refused rather than guessed.
func TestReadOCFMetadata_ForeignWriter(t *testing.T) {
	raw, err := os.ReadFile("testdata/duckdb_v3_manifest_list.avro")
	if err != nil {
		t.Fatal(err)
	}
	meta, err := readOCFMetadata(raw)
	if err != nil {
		t.Fatalf("readOCFMetadata: %v", err)
	}
	if string(meta["avro.codec"]) != "null" {
		t.Errorf("avro.codec = %q, want null", meta["avro.codec"])
	}
	if !strings.Contains(string(meta["avro.schema"]), `"name":"manifest_file"`) {
		t.Errorf("avro.schema does not look like a manifest_file record")
	}
	if _, _, err := readManifestListHeader(raw); err == nil || !strings.Contains(err.Error(), "format-version") {
		t.Errorf("readManifestListHeader: err = %v, want a format-version error", err)
	}
}

// Avro long encoding helpers for hand-built headers.
func avroLong(v int64) []byte { return binary.AppendVarint(nil, v) }

func avroBytes(b []byte) []byte { return append(avroLong(int64(len(b))), b...) }

var ocfMagic = []byte{'O', 'b', 'j', 1}

// ocfHeader builds an OCF header from metadata blocks and a sync marker.
func ocfHeader(blocks ...[]byte) []byte {
	out := append([]byte(nil), ocfMagic...)
	for _, b := range blocks {
		out = append(out, b...)
	}
	out = append(out, avroLong(0)...)
	return append(out, make([]byte, 16)...)
}

func metaBlock(kv ...string) []byte {
	out := avroLong(int64(len(kv) / 2))
	for _, s := range kv {
		out = append(out, avroBytes([]byte(s))...)
	}
	return out
}

const minimalSchema = `{"type":"record","name":"manifest_file","fields":[{"name":"manifest_path","type":"string","field-id":500}]}`

// TestReadManifestListHeader_Malformed feeds hand-built hostile headers:
// each must fail cleanly (no panic, no hang, no large allocation).
func TestReadManifestListHeader_Malformed(t *testing.T) {
	huge := int64(1) << 62
	cases := []struct {
		name string
		raw  []byte
		want string // substring of the error
	}{
		{"empty", nil, ""},
		{"bad magic", append([]byte("Obj\x02"), metaBlock("avro.schema", minimalSchema)...), "magic"},
		{"huge positive block count", append(append([]byte(nil), ocfMagic...), avroLong(huge)...), "exceeds"},
		{"huge negative block count", append(append(append([]byte(nil), ocfMagic...), avroLong(-huge)...), avroLong(8)...), "exceeds"},
		{"min int64 block count", append(append([]byte(nil), ocfMagic...), avroLong(math.MinInt64)...), ""},
		{"block count over data", append(append([]byte(nil), ocfMagic...), avroLong(1<<20)...), ""},
		{"huge key length", append(append(append([]byte(nil), ocfMagic...), avroLong(1)...), avroLong(huge)...), "out of range"},
		{"negative key length", append(append(append([]byte(nil), ocfMagic...), avroLong(1)...), avroLong(-5)...), "out of range"},
		{"huge value length", append(append(append(append([]byte(nil), ocfMagic...), avroLong(1)...), avroBytes([]byte("k"))...), avroLong(huge)...), "out of range"},
		{"huge schema length", append(append(append(append([]byte(nil), ocfMagic...), avroLong(1)...), avroBytes([]byte("avro.schema"))...), avroLong(huge)...), "out of range"},
		{"no schema", ocfHeader(metaBlock("format-version", "2")), "avro.schema"},
		{"no sync marker", append(append(append([]byte(nil), ocfMagic...), metaBlock("avro.schema", minimalSchema)...), avroLong(0)...), "sync"},
		{"missing format-version", ocfHeader(metaBlock("avro.schema", minimalSchema)), "format-version"},
		{"non-numeric format-version", ocfHeader(metaBlock("avro.schema", minimalSchema, "format-version", "two")), "format-version"},
		{"non-numeric snapshot-id", ocfHeader(metaBlock("avro.schema", minimalSchema, "format-version", "2", "snapshot-id", "x")), "snapshot-id"},
		{"overflowing snapshot-id", ocfHeader(metaBlock("avro.schema", minimalSchema, "format-version", "2", "snapshot-id", "9223372036854775808")), "snapshot-id"},
		{"non-numeric parent", ocfHeader(metaBlock("avro.schema", minimalSchema, "format-version", "2", "parent-snapshot-id", "-")), "parent-snapshot-id"},
		{"non-numeric sequence", ocfHeader(metaBlock("avro.schema", minimalSchema, "format-version", "2", "sequence-number", "1.5")), "sequence-number"},
		{"non-numeric first-row-id", ocfHeader(metaBlock("avro.schema", minimalSchema, "format-version", "3", "first-row-id", "x")), "first-row-id"},
		{"schema length at cap, truncated", append(append(append(append([]byte(nil), ocfMagic...), avroLong(1)...), avroBytes([]byte("avro.schema"))...), avroLong(60<<20)...), "EOF"},
		{"many tiny blocks, truncated", append(append([]byte(nil), ocfMagic...), bytes.Repeat(append(avroLong(1), append(avroBytes([]byte("k")), avroBytes([]byte("v"))...)...), 10000)...), "EOF"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, _, err := readManifestListHeaderBounded(t, c.raw)
			if err == nil {
				t.Fatalf("expected an error")
			}
			if c.want != "" && !strings.Contains(err.Error(), c.want) {
				t.Errorf("err = %v, want it to mention %q", err, c.want)
			}
		})
	}
}

// TestReadManifestListHeader_Valid covers the "null" spelling and the
// absent-key defaults on a hand-built header (no data blocks at all).
func TestReadManifestListHeader_Valid(t *testing.T) {
	raw := ocfHeader(
		metaBlock("avro.schema", minimalSchema, "format-version", "2", "snapshot-id", "5"),
		// A second metadata block, encoded with a negative count + byte
		// size, as the Avro spec allows.
		append(append(avroLong(-2), avroLong(0)...), append(avroBytes([]byte("parent-snapshot-id")), append(avroBytes([]byte("null")), append(avroBytes([]byte("sequence-number")), avroBytes([]byte("3"))...)...)...)...),
	)
	hdr, version, err := readManifestListHeader(raw)
	if err != nil {
		t.Fatalf("readManifestListHeader: %v", err)
	}
	want := ManifestListHeader{SnapshotID: 5, SequenceNumber: int64p(3)}
	if version != 2 || !reflect.DeepEqual(hdr, want) {
		t.Errorf("got version %d %s, want 2 %s", version, fmtHeader(hdr), fmtHeader(want))
	}
}

// TestReadManifestListHeader_Truncated cuts a real manifest list at
// every byte inside its header: every cut must be an error, never a
// panic or a silently partial header.
func TestReadManifestListHeader_Truncated(t *testing.T) {
	for _, c := range manifestListCases() {
		raw, _ := buildManifestList(t, c)
		headerLen := ocfHeaderLen(t, raw)
		for n := range headerLen {
			if _, _, err := readManifestListHeaderBounded(t, raw[:n]); err == nil {
				t.Fatalf("%s: header truncated to %d/%d bytes parsed without error", c.name, n, headerLen)
			}
		}
		// The header alone (no data blocks) is enough for the header reader.
		if _, _, err := readManifestListHeader(raw[:headerLen]); err != nil {
			t.Errorf("%s: header-only prefix: %v", c.name, err)
		}
	}
}

// readManifestListHeaderBounded runs readManifestListHeader and fails the
// test if it allocates more than the reader's documented ceiling (the
// 64 MiB avro.schema value cap plus slack) or takes longer than a second.
func readManifestListHeaderBounded(t *testing.T, raw []byte) (ManifestListHeader, int, error) {
	t.Helper()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	start := time.Now()
	hdr, v, err := readManifestListHeader(raw)
	elapsed := time.Since(start)
	runtime.ReadMemStats(&after)
	const maxAlloc = 80 << 20
	if d := after.TotalAlloc - before.TotalAlloc; d > maxAlloc {
		t.Errorf("readManifestListHeader allocated %d bytes on %d input bytes (ceiling %d)", d, len(raw), maxAlloc)
	}
	if elapsed > time.Second {
		t.Errorf("readManifestListHeader took %s on %d input bytes", elapsed, len(raw))
	}
	return hdr, v, err
}

// FuzzReadManifestListHeader: arbitrary input must never panic, and a
// successful parse must report a version that came from the header.
func FuzzReadManifestListHeader(f *testing.F) {
	for _, c := range manifestListCases() {
		buf := &bytes.Buffer{}
		files := []iceberg.ManifestFile{}
		if err := iceberg.WriteManifestList(c.version, buf, c.snapshotID, c.parent, c.seq, c.firstRowID, files); err != nil {
			f.Fatalf("WriteManifestList: %v", err)
		}
		f.Add(buf.Bytes())
	}
	f.Add(ocfHeader(metaBlock("avro.schema", minimalSchema, "format-version", "2")))
	f.Add(append(append([]byte(nil), ocfMagic...), avroLong(math.MaxInt64)...))
	f.Fuzz(func(t *testing.T, raw []byte) {
		hdr, version, err := readManifestListHeader(raw)
		if err != nil {
			return
		}
		meta, err := readOCFMetadata(raw)
		if err != nil {
			t.Fatalf("header parsed but metadata did not: %v", err)
		}
		if got := string(meta["format-version"]); got == "" {
			t.Fatalf("parsed version %d without a format-version key", version)
		}
		_ = hdr
	})
}

func fmtHeader(h ManifestListHeader) string {
	p := func(v *int64) string {
		if v == nil {
			return "nil"
		}
		return strconv.FormatInt(*v, 10)
	}
	return fmt.Sprintf("{snapshot:%d parent:%s seq:%s firstRow:%d}",
		h.SnapshotID, p(h.ParentSnapshotID), p(h.SequenceNumber), h.FirstRowID)
}
