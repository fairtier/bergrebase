package harness

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
)

var (
	bergrbBuildOnce sync.Once
	bergrbBinPath   string
	bergrbBuildErr  error
)

// BuildBergrb compiles cmd/bergrb once per process and returns the path
// to the resulting binary. Subsequent callers reuse the cached path.
//
// Used by scenario 14 (real-subprocess SIGKILL test) — the only e2e
// test that needs to exec bergrb out-of-process. Build artefacts land
// under os.MkdirTemp; nightly CI is short-lived enough that we don't
// bother cleaning up.
func BuildBergrb(t *testing.T) string {
	t.Helper()
	bergrbBuildOnce.Do(func() {
		// os.MkdirTemp (not t.TempDir) on purpose: the binary is
		// cached across the entire test process via sync.Once and
		// reused by every test that runs scenario 14, so the lifetime
		// is process-scoped, not test-scoped.
		dir, err := os.MkdirTemp("", "bergrb-test-") //nolint:usetesting
		if err != nil {
			bergrbBuildErr = fmt.Errorf("mkdir: %w", err)
			return
		}
		bergrbBinPath = filepath.Join(dir, "bergrb")
		cmd := exec.CommandContext(t.Context(), "go", "build", "-o", bergrbBinPath, "github.com/fairtier/bergrebase/cmd/bergrb")
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			bergrbBuildErr = fmt.Errorf("go build cmd/bergrb: %w", err)
		}
	})
	if bergrbBuildErr != nil {
		t.Fatalf("BuildBergrb: %v", bergrbBuildErr)
	}
	return bergrbBinPath
}
