package storage_test

import (
	"encoding/json"
	"raft/pkg/storage"
	"testing"
)

func TestWAL_Persistence(t *testing.T) {
	wal, dir := newTempWAL(t)

	type LogEntry struct {
		Term  uint64
		Index uint64
		Data  []byte
	}

	entry := LogEntry{Term: 1, Index: 1, Data: []byte("persist")}
	data, _ := json.Marshal(entry)
	if err := wal.Write(entry.Index, data); err != nil {
		t.Fatalf("write failed: %v", err)
	}
	wal.Sync()
	wal.Close()

	wal2, err := storage.Open(dir, &storage.Options{
		SegmentSize: 1024 * 1024,
		AllowEmpty:  true,
	})
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	defer wal2.Close()

	readData, err := wal2.Read(1)
	if err != nil {
		t.Fatalf("read failed after reopen: %v", err)
	}

	var got LogEntry
	json.Unmarshal(readData, &got)
	if string(got.Data) != "persist" {
		t.Errorf("expected 'persist', got %q", string(got.Data))
	}
}
