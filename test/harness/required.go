package harness

import (
	"os"
	"testing"
)

// Required reports whether environment gaps must fail the suite rather than
// skip it. GitHub Actions sets CI=true. A skipped oracle reports the same green
// as a passed one, which is how a missing duckdb could leave the oracles unrun.
func Required() bool {
	return os.Getenv("CI") != "" || os.Getenv("BERGREBASE_TEST_REQUIRE_ALL") != ""
}

// SkipOrFail skips t on a developer machine and fails it under Required.
func SkipOrFail(t *testing.T, format string, args ...any) {
	t.Helper()
	if Required() {
		t.Fatalf("not skippable in CI: "+format, args...)
	}
	t.Skipf(format, args...)
}
