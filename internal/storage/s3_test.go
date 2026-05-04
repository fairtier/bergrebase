package storage

import "testing"

func TestParseS3URI(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		bucket  string
		key     string
		wantErr bool
	}{
		{
			name:   "simple key",
			in:     "s3://bucket/path/to/file.parquet",
			bucket: "bucket",
			key:    "path/to/file.parquet",
		},
		{
			name:   "key with spaces and dots",
			in:     "s3://my-bucket/dir/v1.metadata.json",
			bucket: "my-bucket",
			key:    "dir/v1.metadata.json",
		},
		{
			name:    "missing key",
			in:      "s3://bucket/",
			wantErr: true,
		},
		{
			name:    "missing bucket",
			in:      "s3:///key",
			wantErr: true,
		},
		{
			name:    "wrong scheme",
			in:      "https://bucket/key",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b, k, err := ParseS3URI(tt.in)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("ParseS3URI(%q) want error, got bucket=%q key=%q", tt.in, b, k)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseS3URI(%q) unexpected error: %v", tt.in, err)
			}
			if b != tt.bucket || k != tt.key {
				t.Errorf("ParseS3URI(%q) = (%q, %q), want (%q, %q)", tt.in, b, k, tt.bucket, tt.key)
			}
		})
	}
}
