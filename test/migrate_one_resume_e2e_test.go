package test

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/fairtier/bergrebase/internal/catalog"
	"github.com/fairtier/bergrebase/internal/catalog/rest"
	"github.com/fairtier/bergrebase/internal/rewrite"
	"github.com/fairtier/bergrebase/test/harness"
)

// TestMigrateOne_E2E_ResumeAfterSIGKILL: SIGKILL the bergrb process
// mid-rebase, restart with the same arguments, and watch the table
// converge to the same final state as an uninterrupted run.
//
// Flow:
//  1. SeedV2Wide(200) + register + bulk byte copy of the source bytes
//     to the new prefix (the rclone step).
//  2. Spawn the bergrb binary as a subprocess with full migration flags.
//     A polling goroutine watches the target bucket for the first
//     rewritten metadata file; the moment it appears, SIGKILL the
//     process. The catalog pointer is unchanged because the swap is
//     bergrb's last step and never ran.
//  3. Re-spawn the binary with identical arguments. The engine reads
//     from the (untouched) source, re-writes the metadata graph
//     (overwriting whatever the killed run partially produced), and
//     swaps the catalog. D5 idempotency lets the second run be a
//     superset of the first's work.
//  4. Assert the catalog now resolves to the new metadata location and
//     post-swap validation passes.
//
// Race-loss semantics: if the engine completes before the poll catches
// the first PUT (rare, but possible on a fast box), the test logs the
// race and skips the assertion that the kill landed before the swap —
// but it still validates the final state. The convergence contract
// (catalog resolves + ValidateAfterSwap) is the actual acceptance, and
// it holds either way.
//
// Nightly-only (BERGREBASE_NIGHTLY=1) per testing.md:134.
func TestMigrateOne_E2E_ResumeAfterSIGKILL(t *testing.T) {
	harness.MaybeSkipNotNightly(t)
	if testing.Short() {
		t.Skip("skipping testcontainers stack test in short mode")
	}
	ctx := context.Background()
	s := harness.StartStack(ctx, t)
	store := s.StorageClient()

	bergrbBin := harness.BuildBergrb(t)

	const tableName = "orders_resume"
	const numFiles = 200
	const oldTable = "orders_resume_old"
	const newTable = "orders_resume_new"

	seed := harness.SeedV2Wide(ctx, t, store, s.MinIO.TargetBucket, oldTable, numFiles)

	ns := []string{"default"}
	s.CreateNamespace(ctx, t, ns)
	s.RegisterTable(ctx, t, ns, tableName, seed.MetadataURI)

	cat := rest.New(rest.Config{
		URI:       s.Lakekeeper.BaseURI,
		Warehouse: s.Lakekeeper.Warehouse,
	})
	id := catalog.Identifier{Namespace: ns, Name: tableName}
	if _, err := cat.LoadTable(ctx, id); err != nil {
		t.Fatalf("LoadTable pre-rebase: %v", err)
	}

	oldKeyPrefix := "iceberg/db/" + oldTable + "/"
	newKeyPrefix := "iceberg/db/" + newTable + "/"
	for _, srcURI := range seed.AllURIs {
		dstURI := strings.Replace(srcURI, oldKeyPrefix, newKeyPrefix, 1)
		body, err := store.GetObject(ctx, srcURI)
		if err != nil {
			t.Fatalf("get %s: %v", srcURI, err)
		}
		if err := store.PutObject(ctx, dstURI, body); err != nil {
			t.Fatalf("put %s: %v", dstURI, err)
		}
	}

	srcPrefix := "s3://" + s.MinIO.TargetBucket + "/" + oldKeyPrefix
	tgtPrefix := "s3://" + s.MinIO.TargetBucket + "/" + newKeyPrefix
	wantNewLoc := strings.Replace(seed.MetadataURI, oldKeyPrefix, newKeyPrefix, 1)
	// The first thing the engine writes to target is the rewritten
	// manifest (bottom-up walk: manifest → manifest list → metadata.json
	// → catalog swap). Detecting it is the earliest legal kill signal.
	rewrittenManifestURI := strings.Replace(seed.ManifestURI, oldKeyPrefix, newKeyPrefix, 1)

	bergrbArgs := []string{
		"--catalog-uri", s.Lakekeeper.BaseURI,
		"--catalog-warehouse", s.Lakekeeper.Warehouse,
		"--catalog-token-from-env", "BERGREBASE_TEST_TOKEN",
		"--source-prefix", srcPrefix,
		"--target-prefix", tgtPrefix,
		"--source-endpoint", s.MinIO.Endpoint,
		"--source-region", "us-east-1",
		"--source-path-style",
		"--target-endpoint", s.MinIO.Endpoint,
		"--target-region", "us-east-1",
		"--target-path-style",
		"--namespace", "default",
		"--table", tableName,
	}
	bergrbEnv := append(os.Environ(),
		// Lakekeeper in the test stack runs with auth disabled, so any
		// non-empty token satisfies the CLI's "token required" guard
		// while the catalog itself ignores it.
		"BERGREBASE_TEST_TOKEN=anonymous",
		"SOURCE_AWS_ACCESS_KEY_ID="+s.MinIO.AccessKey,
		"SOURCE_AWS_SECRET_ACCESS_KEY="+s.MinIO.SecretKey,
		"TARGET_AWS_ACCESS_KEY_ID="+s.MinIO.AccessKey,
		"TARGET_AWS_SECRET_ACCESS_KEY="+s.MinIO.SecretKey,
	)

	// Phase 1: spawn + race the rewrite + SIGKILL on first PUT.
	cmd := exec.CommandContext(ctx, bergrbBin, bergrbArgs...)
	cmd.Env = bergrbEnv
	cmd.Stdout = os.Stderr // forward bergrb logs to test output for debugging
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("phase 1: spawn bergrb: %v", err)
	}

	pollCtx, pollCancel := context.WithCancel(ctx)
	killed := make(chan bool, 1) // sends true if we sent the kill, false if we gave up
	go func() {
		defer pollCancel()
		// Tight 1ms poll. The kill window between the first PUT and the
		// catalog swap is on the order of tens of ms; ~50 polls usually
		// land inside it.
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-pollCtx.Done():
				killed <- false
				return
			case <-ticker.C:
				if _, err := store.HeadObject(pollCtx, rewrittenManifestURI); err == nil {
					_ = cmd.Process.Kill()
					killed <- true
					return
				}
			}
		}
	}()

	waitErr := cmd.Wait()
	pollCancel()
	wasKilled := <-killed

	mid, err := cat.LoadTable(ctx, id)
	if err != nil {
		t.Fatalf("phase 1: LoadTable post-kill: %v", err)
	}

	switch {
	case wasKilled && mid.MetadataLocation == seed.MetadataURI:
		// Happy path: the kill fired before the catalog swap. waitErr
		// should reflect a SIGKILL exit.
		if !exitedFromSIGKILL(waitErr) {
			t.Errorf("phase 1: process exited %v, expected SIGKILL", waitErr)
		}
		t.Logf("phase 1: bergrb killed before catalog swap; catalog still at oldLocation")
	case wasKilled && mid.MetadataLocation == wantNewLoc:
		// Race lost: the swap committed between our HEAD detection and
		// the SIGKILL landing. End-state is already correct; phase 2
		// becomes a no-op. Test still validates the convergence contract.
		t.Logf("phase 1: race with swap; catalog already at newLocation. Phase 2 will be a no-op.")
	case !wasKilled:
		// We gave up polling because the process exited cleanly first.
		// (Cleanup happens in pollCancel above.) End-state was committed
		// by the binary itself, exactly as if scenario 14 hadn't been
		// asked for. Still flow into phase 2 — re-running with the same
		// args must remain safe per acceptance criterion #3.
		if waitErr != nil {
			t.Fatalf("phase 1: bergrb exited cleanly per poller, but Wait returned %v", waitErr)
		}
		t.Logf("phase 1: poller never fired before bergrb exited 0; first run completed cleanly")
	default:
		t.Fatalf("phase 1: unexpected mid-state: killed=%v, catalog=%q (want either oldLoc or wantNewLoc)",
			wasKilled, mid.MetadataLocation)
	}

	// Phase 2: re-spawn with identical args. Always runs — even if the
	// first run completed it must remain idempotent.
	cmd2 := exec.CommandContext(ctx, bergrbBin, bergrbArgs...)
	cmd2.Env = bergrbEnv
	cmd2.Stdout = os.Stderr
	cmd2.Stderr = os.Stderr
	if err := cmd2.Run(); err != nil {
		t.Fatalf("phase 2: bergrb resume: %v", err)
	}

	reloaded, err := cat.LoadTable(ctx, id)
	if err != nil {
		t.Fatalf("phase 2: LoadTable post-resume: %v", err)
	}
	if reloaded.MetadataLocation != wantNewLoc {
		t.Errorf("phase 2: post-resume pointer: have %q, want %q", reloaded.MetadataLocation, wantNewLoc)
	}
	if err := rewrite.ValidateAfterSwap(ctx, store, wantNewLoc); err != nil {
		t.Errorf("phase 2: ValidateAfterSwap: %v", err)
	}
}

// exitedFromSIGKILL reports whether the *exec.Cmd.Wait() error indicates
// the child was terminated by SIGKILL (signal 9). Used to confirm the
// scenario 14 kill actually delivered the signal — a process that
// exited cleanly mid-rebase would silently degrade the test into "the
// kill never fired".
func exitedFromSIGKILL(err error) bool {
	var ee *exec.ExitError
	if !errors.As(err, &ee) {
		return false
	}
	ws, ok := ee.Sys().(syscall.WaitStatus)
	if !ok {
		return false
	}
	return ws.Signaled() && ws.Signal() == syscall.SIGKILL
}
