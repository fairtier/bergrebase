// Package rewrite contains the core algorithm for rewriting absolute
// object-storage URIs inside Apache Iceberg metadata.
package rewrite

import "strings"

// PrefixMapping describes a single source→target URI substitution.
//
// The substitution is strict: a string is mutated only when it has Source
// as its prefix. Naïve byte replacement would corrupt user data — for
// example, the Avro lower_bounds/upper_bounds byte arrays in a manifest
// describe column values, and a column whose values literally contain the
// source bucket name would be silently mutated. We therefore restrict
// rewriting to known path-bearing fields by name and apply this strict
// prefix substitution to those fields only.
//
// An already-rewritten string (one starting with Target) passes through
// unchanged, which is what makes a re-run idempotent.
type PrefixMapping struct {
	Source string
	Target string
}

// Apply returns p with Source replaced by Target if and only if p starts
// with Source. Otherwise p is returned unchanged.
func (m PrefixMapping) Apply(p string) string {
	if rest, ok := strings.CutPrefix(p, m.Source); ok {
		return m.Target + rest
	}
	return p
}

// Matches reports whether p starts with Source. Useful for callers that
// want to assert a path is in scope for rewriting before recursing into a
// referenced file.
func (m PrefixMapping) Matches(p string) bool {
	return strings.HasPrefix(p, m.Source)
}
