package fsm

import "time"

type EntryType uint8

const (
	EntryNormal EntryType = iota
	EntryConfChange
	EntryBarrier
)

type ApplyMsg struct {
	Index uint64
	Term  uint64
	Type  EntryType
	Data  []byte
}

type SnapshotMeta struct {
	Index    uint64    
	Term     uint64    
	Size     int64     
	Checksum uint32    
	Created  time.Time 
}

type FSMOptions struct {
	StrictIndexCheck bool 
	MaxBatchApply    int  
	MetricsEnabled   bool 
}