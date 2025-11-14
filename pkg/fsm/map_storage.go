package fsm

import "sync"

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

func (ms *MapStorage) Put(key []byte, value []byte) error {
	ms.mu.Lock()
    ms.table[string(key)] = value
    ms.mu.Unlock()
    return nil
}


func (ms *MapStorage) Delete(key []byte) error {
	ms.mu.Lock()
    delete(ms.table, string(key))
    ms.mu.Unlock()
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