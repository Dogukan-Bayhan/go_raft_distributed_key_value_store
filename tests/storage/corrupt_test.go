package storage_test

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWAL_CorruptEntry(t *testing.T) {
	wal, _ := newTempWAL(t)

	if err := wal.Write(1, []byte(`{"index":1,"term":1,"data":"ok"}`)); err != nil {
		t.Fatal(err)
	}

	segPath := filepath.Join(wal.Path, "00000000000000000001")
	f, _ := os.OpenFile(segPath, os.O_APPEND|os.O_WRONLY, 0644)
	f.Write([]byte("{invalid json without newline"))
	f.Close()

	if _, err := wal.Read(2); err == nil {
		t.Errorf("expected error for corrupt entry, got nil")
	}
}
