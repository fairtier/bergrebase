# Running bergrebase as a Kubernetes Job

`bergrebase` is a stateless one-shot binary, run once per migration. The
natural fit is a `batch/v1` Job kicked off by any orchestrator
(controller, GitOps reconciler, ad-hoc operator script) after a bulk
byte-copy Job (e.g. `rclone sync`, `aws s3 sync`, `mc mirror`) has
completed successfully.

## Two ways to apply

- **[`job.example.yaml`](./job.example.yaml)** — single self-contained
  Job manifest with placeholder values. Copy, edit the `CHANGE-ME`
  lines, create the `bergrebase-creds` Secret, then `kubectl apply -f`.

- **[`base/`](./base/)** — Kustomize base. Overlays supply a
  `bergrebase-config` ConfigMap (non-secret values) and a
  `bergrebase-creds` Secret. The header comment in
  [`base/kustomization.yaml`](./base/kustomization.yaml) shows a worked
  overlay.

Resource names (`bergrebase-creds`, `bergrebase-rewrite`,
`bergrebase-config`) are placeholders — rename to fit your conventions.

## Status reporting

An orchestrator can read Job status via the Kubernetes API
(`batch/v1/Job.status`) and map it to whatever user-visible state it
exposes:

- Job `Active > 0` → rewrite in progress.
- Job `Succeeded > 0` → rewrite complete.
- Job `Failed > 0` (with `Failed` condition `True`) → rewrite failed.

`bergrb` exits non-zero on any per-table failure unless `--keep-going`
is passed; in either case the structured JSON logs on stderr name the
failing table.
