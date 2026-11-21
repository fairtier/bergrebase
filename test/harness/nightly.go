package harness

import (
	"os"
	"testing"
)

// MaybeSkipNotNightly skips t unless BERGREBASE_NIGHTLY=1.
//
// Used for the slower perf, resume and large-scale scenarios. A plain local
// `go test ./test/...` stays quick by leaving them out; CI sets the variable,
// so they run on every PR (there is no separate nightly workflow).
func MaybeSkipNotNightly(t *testing.T) {
	t.Helper()
	if os.Getenv("BERGREBASE_NIGHTLY") == "" {
		SkipOrFail(t, "nightly-only; set BERGREBASE_NIGHTLY=1 to run")
	}
}
