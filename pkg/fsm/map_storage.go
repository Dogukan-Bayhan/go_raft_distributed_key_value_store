package fsm

import (
	"bytes"
	"sync"
)

type MapStorage struct {
	mu sync.RWMutex
	table map[string][]byte
}



func (ms *MapStorage) Get(key []byte) ([]byte, bool, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	value, ok := ms.table[string(key)]

	return value, ok, nil
}

func (ms *MapStorage) Put(key, value []byte) error {
    ms.mu.Lock()
    defer ms.mu.Unlock()

    ms.table[string(key)] = append([]byte(nil), value...)
    return nil
}

func (ms *MapStorage) CAS(key []byte, expected []byte ,value []byte) (error, bool) {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	val, ok := ms.table[string(key)]
	
	if ok && bytes.Equal(val, expected) {
		ms.table[string(key)] = append([]byte(nil), value...)
		return nil, true
	}

	return nil, false
}


func (ms *MapStorage) Delete(key []byte) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()
    delete(ms.table, string(key))
    return nil
}

func (ms *MapStorage) Snaphsot() (map[string][]byte, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	snap := make(map[string][]byte, len(ms.table))
	for k, v := range ms.table {
		snap[k] = append([]byte(nil), v...)
	}
	return snap, nil

}

func (ms *MapStorage) Restore(data map[string][]byte) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	ms.table = make(map[string][]byte)
	for k, v := range data {
		ms.table[k] = append([]byte(nil), v...)
	}
	return nil
}

func (ms *MapStorage) Flush() error {
	return nil
}

func (ms *MapStorage) Close() error {
	return nil
}