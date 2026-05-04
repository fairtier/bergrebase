package rewrite

import "testing"

func TestPrefixMapping_Apply(t *testing.T) {
	m := PrefixMapping{
		Source: "s3://old-bucket/iceberg/",
		Target: "s3://new-bucket/iceberg/",
	}

	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "rewrites matching prefix",
			in:   "s3://old-bucket/iceberg/db/orders/data/file.parquet",
			want: "s3://new-bucket/iceberg/db/orders/data/file.parquet",
		},
		{
			name: "idempotent on already-rewritten paths",
			in:   "s3://new-bucket/iceberg/db/orders/data/file.parquet",
			want: "s3://new-bucket/iceberg/db/orders/data/file.parquet",
		},
		{
			name: "leaves unrelated prefixes alone",
			in:   "s3://different-bucket/foo/bar",
			want: "s3://different-bucket/foo/bar",
		},
		{
			name: "leaves empty input alone",
			in:   "",
			want: "",
		},
		{
			name: "leaves substring match (not prefix) alone",
			in:   "https://example.com/?ref=s3://old-bucket/iceberg/",
			want: "https://example.com/?ref=s3://old-bucket/iceberg/",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := m.Apply(tt.in); got != tt.want {
				t.Errorf("Apply(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

func TestPrefixMapping_Matches(t *testing.T) {
	m := PrefixMapping{
		Source: "s3://old/",
		Target: "s3://new/",
	}
	if !m.Matches("s3://old/foo") {
		t.Error("expected match for s3://old/foo")
	}
	if m.Matches("s3://other/foo") {
		t.Error("did not expect match for s3://other/foo")
	}
}
