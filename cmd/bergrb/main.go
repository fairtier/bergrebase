// Command bergrebase rewrites absolute object-storage URIs
// inside Apache Iceberg metadata after a bulk byte copy from one
// S3-compatible bucket to another, then re-points the catalog at the
// rewritten metadata.
//
// See ../../README.md for usage.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"github.com/fairtier/bergrebase/internal/catalog"
	"github.com/fairtier/bergrebase/internal/catalog/rest"
	"github.com/fairtier/bergrebase/internal/rewrite"
	"github.com/fairtier/bergrebase/internal/storage"
)

type config struct {
	catalogURI       string
	catalogWarehouse string
	catalogTokenEnv  string

	sourcePrefix    string
	sourceEndpoint  string
	sourceRegion    string
	sourcePathStyle bool

	targetPrefix    string
	targetEndpoint  string
	targetRegion    string
	targetPathStyle bool

	namespace     string
	table         string
	allTables     bool
	allNamespaces bool
	dryRun        bool
	currentOnly   bool
	keepGoing     bool
	noValidate    bool

	maxObjectSize int64
}

func parseFlags() (*config, error) {
	c := &config{}

	flag.StringVar(&c.catalogURI, "catalog-uri", "", "Iceberg REST catalog base URI (required)")
	flag.StringVar(&c.catalogWarehouse, "catalog-warehouse", "", "warehouse name or UUID (required)")
	flag.StringVar(&c.catalogTokenEnv, "catalog-token-from-env", "LAKEKEEPER_TOKEN", "env var holding the bearer token (leave empty for unauthenticated catalogs)")

	flag.StringVar(&c.sourcePrefix, "source-prefix", "", "source URI prefix, e.g. s3://old-bucket/iceberg/ (required)")
	flag.StringVar(&c.sourceEndpoint, "source-endpoint", "", "S3 endpoint for source (omit for AWS S3)")
	flag.StringVar(&c.sourceRegion, "source-region", "", "S3 region for source")
	flag.BoolVar(&c.sourcePathStyle, "source-path-style", false, "use path-style addressing for source")

	flag.StringVar(&c.targetPrefix, "target-prefix", "", "target URI prefix, e.g. s3://new-bucket/iceberg/ (required)")
	flag.StringVar(&c.targetEndpoint, "target-endpoint", "", "S3 endpoint for target (omit for AWS S3)")
	flag.StringVar(&c.targetRegion, "target-region", "", "S3 region for target")
	flag.BoolVar(&c.targetPathStyle, "target-path-style", false, "use path-style addressing for target")

	flag.StringVar(&c.namespace, "namespace", "", "Iceberg namespace (dot-separated; required unless --all-namespaces)")
	flag.StringVar(&c.table, "table", "", "single table to migrate (mutually exclusive with --all-tables)")
	flag.BoolVar(&c.allTables, "all-tables", false, "migrate every table in the namespace")
	flag.BoolVar(&c.allNamespaces, "all-namespaces", false, "rebase every table in every namespace in the warehouse (mutually exclusive with --namespace, --table, --all-tables)")

	flag.Int64Var(&c.maxObjectSize, "max-object-size", 0, "cap in bytes on any single object read; 0 uses the default (256 MiB). Raise for very large metadata.json or position-delete files")

	flag.BoolVar(&c.dryRun, "dry-run", false, "walk the metadata graph and report planned changes; no writes")
	flag.BoolVar(&c.currentOnly, "current-snapshot-only", false, "skip historical snapshots (breaks time travel)")
	flag.BoolVar(&c.keepGoing, "keep-going", false, "continue past per-table failures")
	flag.BoolVar(&c.noValidate, "no-validate", false, "skip post-migration LoadTable + HEAD validation")

	flag.Parse()

	if err := c.validate(); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *config) validate() error {
	missing := []string{}
	if c.catalogURI == "" {
		missing = append(missing, "--catalog-uri")
	}
	if c.catalogWarehouse == "" {
		missing = append(missing, "--catalog-warehouse")
	}
	if c.sourcePrefix == "" {
		missing = append(missing, "--source-prefix")
	}
	if c.targetPrefix == "" {
		missing = append(missing, "--target-prefix")
	}
	if !c.allNamespaces && c.namespace == "" {
		missing = append(missing, "--namespace")
	}
	if len(missing) > 0 {
		return fmt.Errorf("missing required flags: %s", strings.Join(missing, ", "))
	}
	if c.allNamespaces {
		conflicts := []string{}
		if c.namespace != "" {
			conflicts = append(conflicts, "--namespace")
		}
		if c.table != "" {
			conflicts = append(conflicts, "--table")
		}
		if c.allTables {
			conflicts = append(conflicts, "--all-tables")
		}
		if len(conflicts) > 0 {
			return fmt.Errorf("--all-namespaces is mutually exclusive with: %s",
				strings.Join(conflicts, ", "))
		}
	} else {
		hasTable := c.table != ""
		if hasTable == c.allTables {
			return errors.New("exactly one of --table or --all-tables is required")
		}
	}
	if c.sourcePrefix == c.targetPrefix {
		return errors.New("--source-prefix and --target-prefix must differ")
	}
	// A slash-less prefix also matches sibling paths ("s3://b/warehouse"
	// matches "s3://b/warehouse2/…"), silently rebasing tables the bulk
	// copy never covered.
	if !strings.HasSuffix(c.sourcePrefix, "/") {
		return fmt.Errorf("--source-prefix %q must end with '/'", c.sourcePrefix)
	}
	if !strings.HasSuffix(c.targetPrefix, "/") {
		return fmt.Errorf("--target-prefix %q must end with '/'", c.targetPrefix)
	}
	// Nested prefixes break idempotency: with the target under the
	// source, every rewritten path still matches the source prefix and a
	// re-run rewrites it again (s3://b/x/deep/deep/…).
	if strings.HasPrefix(c.targetPrefix, c.sourcePrefix) || strings.HasPrefix(c.sourcePrefix, c.targetPrefix) {
		return errors.New("--source-prefix and --target-prefix must not be nested one under the other")
	}
	if c.maxObjectSize < 0 {
		return errors.New("--max-object-size must be >= 0")
	}
	return nil
}

func main() {
	os.Exit(realMain())
}

func realMain() int {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	cfg, err := parseFlags()
	if err != nil {
		logger.Error("flag error", "err", err)
		return 2
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if err := run(ctx, cfg); err != nil {
		logger.Error("rewrite failed", "err", err)
		return 1
	}
	return 0
}

func run(ctx context.Context, cfg *config) error {
	// An empty token is fine if and only if the catalog runs
	// unauthenticated (typical for dev/test stacks). We don't gate on
	// it here — production catalogs will return their own 401, which
	// is a clearer error than a synthetic "no token" message that
	// can't tell which catalogs need one. cfg.dryRun has no bearing on
	// auth: dry-run still calls LoadTable / ListTables.
	token := os.Getenv(cfg.catalogTokenEnv)
	if token != "" && strings.HasPrefix(cfg.catalogURI, "http://") {
		slog.Warn("catalog URI uses plain http with a bearer token; the token travels in cleartext",
			"catalog_uri", cfg.catalogURI)
	}

	cat := rest.New(rest.Config{
		URI:       cfg.catalogURI,
		Warehouse: cfg.catalogWarehouse,
		Token:     token,
	})

	sourceCreds, err := sideCredentials("SOURCE")
	if err != nil {
		return err
	}
	targetCreds, err := sideCredentials("TARGET")
	if err != nil {
		return err
	}

	source := storage.New(storage.Config{
		Region:          cfg.sourceRegion,
		Endpoint:        cfg.sourceEndpoint,
		PathStyle:       cfg.sourcePathStyle,
		AccessKeyID:     sourceCreds.accessKeyID,
		SecretAccessKey: sourceCreds.secretAccessKey,
		SessionToken:    sourceCreds.sessionToken,
		MaxObjectSize:   cfg.maxObjectSize,
	})

	target := storage.New(storage.Config{
		Region:          cfg.targetRegion,
		Endpoint:        cfg.targetEndpoint,
		PathStyle:       cfg.targetPathStyle,
		AccessKeyID:     targetCreds.accessKeyID,
		SecretAccessKey: targetCreds.secretAccessKey,
		SessionToken:    targetCreds.sessionToken,
		MaxObjectSize:   cfg.maxObjectSize,
	})

	engine := &rewrite.Engine{
		Source: source,
		Target: target,
		Opts: rewrite.Options{
			Mapping: rewrite.PrefixMapping{
				Source: cfg.sourcePrefix,
				Target: cfg.targetPrefix,
			},
			CurrentSnapshotOnly: cfg.currentOnly,
			DryRun:              cfg.dryRun,
		},
	}

	batches, err := selectBatches(ctx, cat, cfg)
	if err != nil {
		return fmt.Errorf("select tables: %w", err)
	}

	var (
		firstErr     error
		totalOK      int
		totalFailed  int
		nsCount      int
		multiSummary = cfg.allNamespaces
	)
	for _, batch := range batches {
		nsCount++
		var nsOK, nsFailed int
		for _, id := range batch.tables {
			if err := migrateOne(ctx, cat, engine, target, id, cfg); err != nil {
				slog.Error("table migration failed", "table", qualifiedName(id), "err", err)
				nsFailed++
				if firstErr == nil {
					firstErr = err
				}
				if !cfg.keepGoing {
					if multiSummary {
						slog.Info(
							"namespace summary",
							"namespace", namespaceLabel(batch.ns),
							"rebased", nsOK,
							"failed", nsFailed,
						)
					}
					return err
				}
				continue
			}
			nsOK++
			slog.Info("table migrated", "table", qualifiedName(id))
		}
		totalOK += nsOK
		totalFailed += nsFailed
		if multiSummary {
			slog.Info(
				"namespace summary",
				"namespace", namespaceLabel(batch.ns),
				"rebased", nsOK,
				"failed", nsFailed,
			)
		}
	}
	if multiSummary {
		slog.Info(
			"warehouse summary",
			"namespaces", nsCount,
			"rebased", totalOK,
			"failed", totalFailed,
		)
	}
	return firstErr
}

// awsCredentials is one side's static credential triple.
type awsCredentials struct {
	accessKeyID     string
	secretAccessKey string
	sessionToken    string
}

// sideCredentials reads the <SIDE>_AWS_* env triple for one side of the
// migration and logs which credential source that side will use. In a
// two-account migration tool a silent fall-through to the SDK default
// chain (env AWS_*, shared config, IMDS) can sign writes with the wrong
// identity, so the fallback is loud, and a half-set key pair — almost
// always a typo in one of the variable names — is an error rather than
// a silent fallback.
func sideCredentials(side string) (awsCredentials, error) {
	creds := awsCredentials{
		accessKeyID:     os.Getenv(side + "_AWS_ACCESS_KEY_ID"),
		secretAccessKey: os.Getenv(side + "_AWS_SECRET_ACCESS_KEY"),
		sessionToken:    os.Getenv(side + "_AWS_SESSION_TOKEN"),
	}
	if (creds.accessKeyID == "") != (creds.secretAccessKey == "") {
		return awsCredentials{}, fmt.Errorf(
			"%s_AWS_ACCESS_KEY_ID and %s_AWS_SECRET_ACCESS_KEY must be set together (exactly one is set — typo in the variable name?)",
			side, side)
	}
	if creds.accessKeyID == "" {
		slog.Warn("no static credentials in env; using the AWS SDK default chain (env AWS_*, shared config, IMDS)",
			"side", strings.ToLower(side), "env_vars", side+"_AWS_ACCESS_KEY_ID/"+side+"_AWS_SECRET_ACCESS_KEY")
	} else {
		slog.Info("using static credentials from env",
			"side", strings.ToLower(side), "access_key_id_env", side+"_AWS_ACCESS_KEY_ID")
	}
	return creds, nil
}

// nsBatch groups the tables to rebase in a single namespace.
type nsBatch struct {
	ns     catalog.Namespace
	tables []catalog.Identifier
}

func selectBatches(ctx context.Context, cat catalog.Catalog, cfg *config) ([]nsBatch, error) {
	if cfg.allNamespaces {
		all, err := listAllNamespaces(ctx, cat)
		if err != nil {
			return nil, err
		}
		batches := make([]nsBatch, 0, len(all))
		for _, ns := range all {
			tables, err := cat.ListTables(ctx, ns)
			if err != nil {
				return nil, fmt.Errorf("list tables in %s: %w", namespaceLabel(ns), err)
			}
			batches = append(batches, nsBatch{ns: ns, tables: tables})
		}
		return batches, nil
	}
	ns := strings.Split(cfg.namespace, ".")
	if cfg.allTables {
		tables, err := cat.ListTables(ctx, ns)
		if err != nil {
			return nil, err
		}
		return []nsBatch{{ns: ns, tables: tables}}, nil
	}
	return []nsBatch{{
		ns:     ns,
		tables: []catalog.Identifier{{Namespace: ns, Name: cfg.table}},
	}}, nil
}

// listAllNamespaces walks the warehouse breadth-first, returning every
// namespace (root and nested). Tables can live at any level so the caller
// must call ListTables on each namespace returned, not only on leaves.
//
// The visited set guards against a buggy catalog listing a namespace
// under itself (or any other cycle), which would otherwise loop forever
// and grow the queue unboundedly.
func listAllNamespaces(ctx context.Context, cat catalog.Catalog) ([]catalog.Namespace, error) {
	roots, err := cat.ListNamespaces(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("list root namespaces: %w", err)
	}
	all := make([]catalog.Namespace, 0, len(roots))
	visited := make(map[string]bool, len(roots))
	queue := append([]catalog.Namespace(nil), roots...)
	for len(queue) > 0 {
		ns := queue[0]
		queue = queue[1:]
		// Join with the same U+001F separator the REST spec uses for
		// path segments — it cannot appear inside a namespace element,
		// so the key is collision-free (unlike ".").
		key := strings.Join(ns, "\x1f")
		if visited[key] {
			continue
		}
		visited[key] = true
		all = append(all, ns)
		children, err := cat.ListNamespaces(ctx, ns)
		if err != nil {
			return nil, fmt.Errorf("list child namespaces of %s: %w", namespaceLabel(ns), err)
		}
		queue = append(queue, children...)
	}
	return all, nil
}

func namespaceLabel(ns catalog.Namespace) string {
	if len(ns) == 0 {
		return "<root>"
	}
	return strings.Join(ns, ".")
}

func migrateOne(ctx context.Context, cat catalog.Catalog, engine *rewrite.Engine, target rewrite.Storage, id catalog.Identifier, cfg *config) error {
	tbl, err := cat.LoadTable(ctx, id)
	if err != nil {
		return fmt.Errorf("load table: %w", err)
	}

	// After a successful swap the pointer starts with the target prefix.
	// Treat that as "already migrated" rather than failing the
	// source-prefix check inside RewriteTable, so a re-run over a
	// partially migrated namespace converges instead of erroring on the
	// tables that already made it across.
	if strings.HasPrefix(tbl.MetadataLocation, cfg.targetPrefix) {
		slog.Info("table already migrated; skipping",
			"table", qualifiedName(id), "metadata_location", tbl.MetadataLocation)
		return nil
	}

	res, err := engine.RewriteTable(ctx, tbl.MetadataLocation)
	if err != nil {
		return fmt.Errorf("rewrite metadata: %w", err)
	}
	slog.Info(
		"metadata rewritten",
		"old", res.OldMetadataLocation,
		"new", res.NewMetadataLocation,
		"manifest_lists", res.ManifestListsRewritten,
		"manifests", res.ManifestsRewritten,
		"stats_files", res.StatsFilesRewritten,
	)

	if cfg.dryRun {
		return nil
	}

	if err := cat.SwapMetadataLocation(ctx, id, tbl.MetadataLocation, res.NewMetadataLocation); err != nil {
		return fmt.Errorf("swap catalog pointer: %w", err)
	}

	if cfg.noValidate {
		return nil
	}
	if err := validateAfterSwap(ctx, cat, target, id, res.NewMetadataLocation, tbl.CurrentSnapshotID); err != nil {
		return fmt.Errorf("post-swap validation: %w", err)
	}
	return nil
}

// validateAfterSwap performs the catalog-side and storage-side halves
// of the post-rebase validation. It is best-effort: a true correctness
// proof would re-read every data file's footer and compare per-column
// bounds.
func validateAfterSwap(ctx context.Context, cat catalog.Catalog, target rewrite.Storage, id catalog.Identifier, expectedLoc string, expectedSnap *int64) error {
	reloaded, err := cat.LoadTable(ctx, id)
	if err != nil {
		return fmt.Errorf("reload after swap: %w", err)
	}
	if reloaded.MetadataLocation != expectedLoc {
		return fmt.Errorf("catalog pointer drifted: have %q, expected %q",
			reloaded.MetadataLocation, expectedLoc)
	}
	if !snapshotIDsEqual(reloaded.CurrentSnapshotID, expectedSnap) {
		return fmt.Errorf("current-snapshot-id drifted: have %v, expected %v",
			snapshotID(reloaded.CurrentSnapshotID), snapshotID(expectedSnap))
	}
	return rewrite.ValidateAfterSwap(ctx, target, expectedLoc)
}

func snapshotIDsEqual(a, b *int64) bool {
	switch {
	case a == nil && b == nil:
		return true
	case a == nil || b == nil:
		return false
	default:
		return *a == *b
	}
}

func snapshotID(p *int64) string {
	if p == nil {
		return "<nil>"
	}
	return strconv.FormatInt(*p, 10)
}

func qualifiedName(id catalog.Identifier) string {
	return strings.Join(append(id.Namespace, id.Name), ".")
}
