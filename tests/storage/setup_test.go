package storage_test

import (
	"testing"
	"time"

	"raft/pkg/storage"
)

func newTempWAL(t *testing.T) (*storage.WAL, string) {
	t.Helper()

	tmpDir := t.TempDir() // otomatik olarak test bitince silinir
	opts := storage.Options{
		SegmentSize: 1024 * 1024,
		AllowEmpty:  true,
		NoSync:      true,
		FilePerms:   0644,
	}

	wal, err := storage.Open(tmpDir, &opts)
	if err != nil {
		t.Fatalf("failed to open WAL: %v", err)
	}

	t.Cleanup(func() {
        wal.Close()
        time.Sleep(50 * time.Millisecond) 
    })

	return wal, tmpDir
}
