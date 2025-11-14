package fsm

import (
	"os"
	"sync"
)

type BTreeStorage struct {
	Mu sync.RWMutex
	// tree *btree.BTree
	File *os.File
}