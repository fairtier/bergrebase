package harness

import (
	"os"
	"testing"
)

// MaybeSkipNotNightly skips t unless BERGREBASE_NIGHTLY=1.
//
// Used for the perf and resume scenarios that are deferred to nightly
// CI: failures open an issue but do not block merges. The default `go
// test ./test/...` run keeps these tests out of the merge-gate path
// while leaving them in the package so they continue to compile and
// type-check.
func MaybeSkipNotNightly(t *testing.T) {
	t.Helper()
	if os.Getenv("BERGREBASE_NIGHTLY") == "" {
		t.Skip("nightly-only; set BERGREBASE_NIGHTLY=1 to run")
	}
}
