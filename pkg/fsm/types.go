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

type Metrics struct {
    TotalKeys           uint64
    TotalBytes          uint64
    TTLKeys             uint64
    
    OpsTotal            uint64
    OpsPut              uint64
    OpsDelete           uint64
    OpsGetLeader        uint64
    OpsGetFollower      uint64
    OpsExpired          uint64
    
    SnapshotCount       uint64
    SnapshotLastSize    uint64
        
    ApplyCount          uint64
    ApplyAvgLatencyNs   uint64
}


type Storage interface {
	Get(key []byte) ([]byte, bool, error)
}

type FSM struct {
	Storage *Storage
	Metrics *Metrics
}