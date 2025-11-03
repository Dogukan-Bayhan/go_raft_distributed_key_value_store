package storage_test

import (
	"testing"
)


func TestWAL_CreateEmpty(t *testing.T) {
	wal, _ := newTempWAL(t)
	defer wal.Close()

	first, err1 := wal.FirstIndex()
	last, err2 := wal.LastIndex()

	if err1 != nil || err2 != nil {
		t.Fatalf("unexpected error: %v, %v", err1, err2)
	}

	if first != 1 || last != 0 {
		t.Errorf("expected first=0 last=0, got %d %d", first, last)
	}
}
