package rewrite

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// truncatingStorage returns a HeadObject size that disagrees with the
// PutObject body, simulating silent S3 truncation.
type truncatingStorage struct {
	*memStorage
	reportedSize int64
}

func (t *truncatingStorage) HeadObject(_ context.Context, _ string) (int64, error) {
	return t.reportedSize, nil
}

func TestPutAndVerify_DetectsTruncation(t *testing.T) {
	store := &truncatingStorage{memStorage: newMemStorage(), reportedSize: 5}
	body := []byte("0123456789") // 10 bytes; HeadObject will lie and say 5

	err := putAndVerify(context.Background(), store, "s3://b/k", body)
	if err == nil {
		t.Fatal("expected verify error, got nil")
	}
	if !strings.Contains(err.Error(), "on-disk size 5 != written size 10") {
		t.Errorf("error message lacks truncation detail: %v", err)
	}
}

// erroringStorage fails on PutObject.
type erroringStorage struct {
	*memStorage
}

func (e *erroringStorage) PutObject(_ context.Context, _ string, _ []byte) error {
	return errors.New("put failed: 503")
}

func TestPutAndVerify_PutErrorIsReturned(t *testing.T) {
	store := &erroringStorage{memStorage: newMemStorage()}
	err := putAndVerify(context.Background(), store, "s3://b/k", []byte("body"))
	if err == nil {
		t.Fatal("expected put error")
	}
	if !strings.Contains(err.Error(), "put s3://b/k") {
		t.Errorf("error lacks context: %v", err)
	}
}

func TestPutAndVerify_HappyPath(t *testing.T) {
	store := newMemStorage()
	body := []byte("hello world")
	if err := putAndVerify(context.Background(), store, "s3://b/k", body); err != nil {
		t.Fatalf("putAndVerify: %v", err)
	}
	if got := store.objects["s3://b/k"]; string(got) != "hello world" {
		t.Errorf("stored body = %q", got)
	}
}
