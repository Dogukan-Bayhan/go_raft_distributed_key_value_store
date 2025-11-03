package storage_test

import (
	"os"
	"raft/pkg/storage"
	"testing"
)


func TestWAL_SegmentRotation(t *testing.T) {
	opts := storage.Options{
		SegmentSize: 1024, 
		AllowEmpty:  true,
		NoSync:      true,
	}

	dir := t.TempDir()
	wal, err := storage.Open(dir, &opts)
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()

	data := make([]byte, 800) 
	for i := 1; i <= 5; i++ {
		if err := wal.Write(uint64(i), data); err != nil {
			t.Fatal(err)
		}
	}

	files, _ := os.ReadDir(dir)
	if len(files) <= 1 {
		t.Errorf("expected multiple segments, got %d", len(files))
	}
}
