package fsm

import (
	"os"
	"sync"
)

type BTreeStorage struct {
	mu sync.RWMutex
	// tree *btree.BTree
	file *os.File
}