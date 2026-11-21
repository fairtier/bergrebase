#!/usr/bin/env bash
# Installs the writers and readers the e2e suite drives into <dir> and prints the
# environment to run the suite with. The DuckDB CLIs are pinned and checksum-
# verified. pyiceberg and its direct dependencies are pinned by version only (see
# requirements-pyiceberg.txt); their transitive dependencies are not pinned.
#
#   eval "$(test/tools/install-test-tools.sh ~/.cache/bergrebase-test-tools)"
#   go test ./test/...
#
# DuckDB 1.5.4 is here on purpose: it is the last writer with the delete-manifest
# header bug bergrebase has to tolerate, and it stays in the matrix after newer
# versions ship. Linux amd64 only.
set -euo pipefail

dir=${1:?usage: install-test-tools.sh <dir>}
here=$(cd "$(dirname "$0")" && pwd)
mkdir -p "$dir"
dir=$(cd "$dir" && pwd)

fetch_duckdb() { # version asset sha256
  local v=$1 asset=$2 sum=$3
  local d="$dir/duckdb-$v"
  [[ -x "$d/duckdb" ]] && return 0
  mkdir -p "$d"
  curl -fsSL --retry 3 -o "$d/$asset" "https://github.com/duckdb/duckdb/releases/download/v$v/$asset"
  echo "$sum  $d/$asset" | sha256sum --check --quiet - >&2
  case "$asset" in
    *.zip) python3 -c 'import sys, zipfile; zipfile.ZipFile(sys.argv[1]).extractall(sys.argv[2])' "$d/$asset" "$d" ;;
    *) tar -xzf "$d/$asset" -C "$d" ;;
  esac
  chmod +x "$d/duckdb"
  rm -f "${d:?}/${asset:?}"
}

fetch_duckdb 1.5.4 duckdb_cli-linux-amd64.zip 1f2fa724fb054b3dbe1a9cbd13de5b76997d850e7087ec762ba88db04e0180cf
fetch_duckdb 1.5.6 duckdb_cli-linux-amd64.zip 6e89deac1ebbc36eed0291caf8b567b030c7b86ac35998f71854e22b3c5d5e2f
fetch_duckdb 2.0.0 duckdb-cli-linux-amd64.tar.gz c6507206baf7793c6013dbd82368469a45f20dc0e17a6f92f204602f652ed2bc

venv="$dir/pyiceberg"
if [[ ! -x "$venv/bin/python" ]]; then
  if command -v uv >/dev/null; then
    uv venv -q --python 3.12 "$venv" >&2
    uv pip install -q --python "$venv/bin/python" -r "$here/../harness/requirements-pyiceberg.txt" >&2
  else
    python3 -m venv "$venv"
    "$venv/bin/pip" install --quiet -r "$here/../harness/requirements-pyiceberg.txt" >&2
  fi
fi

echo "export BERGREBASE_TEST_DUCKDB='$dir/duckdb-1.5.6/duckdb'"
echo "export BERGREBASE_TEST_DUCKDB_WRITERS='$dir/duckdb-1.5.4/duckdb,$dir/duckdb-1.5.6/duckdb,$dir/duckdb-2.0.0/duckdb'"
echo "export BERGREBASE_TEST_PYTHON='$venv/bin/python'"
