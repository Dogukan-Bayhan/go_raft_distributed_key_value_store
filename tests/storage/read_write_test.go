package storage_test

import (
	"encoding/json"
	"testing"
)


func TestWAL_WriteRead(t *testing.T) {
	wal, _ := newTempWAL(t)

	type LogEntry struct {
		Term  uint64
		Index uint64
		Data  []byte
	}

	entries := []LogEntry{
		{Term: 1, Index: 1, Data: []byte("alpha")},
		{Term: 1, Index: 2, Data: []byte("bravo")},
		{Term: 2, Index: 3, Data: []byte("charlie")},
	}

	for _, e := range entries {
		data, _ := json.Marshal(e)
		if err := wal.Write(e.Index, data); err != nil {
			t.Fatalf("failed to write index %d: %v", e.Index, err)
		}
	}

	for _, e := range entries {
		data, err := wal.Read(e.Index)
		if err != nil {
			t.Fatalf("failed to read index %d: %v", e.Index, err)
		}

		var got LogEntry
		json.Unmarshal(data, &got)

		if got.Term != e.Term || string(got.Data) != string(e.Data) {
			t.Errorf("entry mismatch at index %d: want %+v, got %+v", e.Index, e, got)
		}
	}
}
