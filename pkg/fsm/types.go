package fsm

import "time"

//
// ------------------------------
// Entry Types
// ------------------------------
// EntryType categorizes Raft log entries that the FSM receives.
// Normal entries contain user-level commands (PUT, DELETE, CAS, etc.).
// ConfChange entries are used for membership changes (adding/removing nodes).
// Barrier entries represent synchronization or ReadIndex barriers.
//
type EntryType uint8

const (
    EntryNormal EntryType = iota // Standard client command
    EntryConfChange              // Raft cluster membership change
    EntryBarrier                 // Sync/Barrier entry for linearizable reads
)

//
// ------------------------------
// Apply Message
// ------------------------------
// ApplyMsg is delivered by the Raft layer when a log entry is committed and
// ready to be applied to the FSM. The FSM must apply entries strictly in
// ascending Index order to maintain determinism across all nodes.
//
type ApplyMsg struct {
    Index uint64     // Raft log index of this entry
    Term  uint64     // Raft term when the entry was appended
    Type  EntryType  // Type of entry (Normal, ConfChange, Barrier)
    Data  []byte     // Serialized command payload (PUT/DELETE/CAS/etc.)
}

//
// ------------------------------
// Snapshot Metadata
// ------------------------------
// SnapshotMeta describes the snapshot produced by the FSM. This metadata is
// encoded into the Raft snapshot file and used during restoration.
//
// Index:    Last applied log index included in the snapshot
// Term:     Raft term associated with the last included entry
// Size:     Snapshot size in bytes (informational/monitoring)
// Checksum: Optional checksum for corruption detection
// Created:  Wall-clock timestamp when snapshot was created
//
type SnapshotMeta struct {
    Index    uint64
    Term     uint64
    Size     int64
    Checksum uint32
    Created  time.Time
}

//
// ------------------------------
// FSM Options
// ------------------------------
// FSMOptions configures operational behavior of the FSM.
//
// StrictIndexCheck: If true, FSM enforces monotonic apply index ordering.
// MaxBatchApply:    Max number of entries to apply in a single batch call.
// MetricsEnabled:   Enables/disables metrics collection.
//
type FSMOptions struct {
    StrictIndexCheck bool
    MaxBatchApply    int
    MetricsEnabled   bool
}

//
// ------------------------------
// Metrics
// ------------------------------
// Metrics tracks operational statistics for the FSM and storage backend.
//
// TotalKeys / TotalBytes: Current size of the KV dataset.
// TTLKeys:                Number of keys with TTL metadata.
//
// OpsTotal:     Total number of write operations applied.
// OpsPut:       Number of PUT operations.
// OpsDelete:    Number of DELETE operations.
// OpsGetLeader: GET requests served by the leader (linearizable).
// OpsGetFollower: GET requests served by followers (possibly stale).
// OpsExpired:   Number of TTL expirations applied.
//
// SnapshotCount:     Total snapshots generated.
// SnapshotLastSize:  Size of the last snapshot.
//
// ApplyCount:        Total number of Raft entries applied.
// ApplyAvgLatencyNs: Moving average latency of apply operations.
//
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

//
// ------------------------------
// Storage Interface
// ------------------------------
// Storage is the abstraction layer for the underlying KV engine used by the FSM.
//
// Get/Put/Delete:  Core key-value operations applied deterministically.
// Snapshot():      Returns a full copy of the KV state for Raft snapshotting.
// Restore():       Loads a snapshot into the storage backend.
// Flush():         Persists pending writes if the backend is buffered.
// Close():         Cleans up resources on shutdown.
//
// This interface can represent an in-memory map, B-Tree, LSM-tree segment,
// on-disk SSTable, or any deterministic storage backend.
//
type Storage interface {
    Get(key []byte) ([]byte, bool, error)
    Put(key []byte, value []byte) error
    Delete(key []byte) error
    CAS(key []byte, expected []byte ,value []byte) (error, bool)

    Snaphsot() (map[string][]byte, error)
    Restore(data map[string][]byte) error

    Flush() error
    Close() error
}

//
// ------------------------------
// FSM (Finite State Machine)
// ------------------------------
// FSM is the core deterministic state machine used by Raft. Every committed
// Raft log entry flows through this FSM. Its responsibilities include:
//
// - Decoding commands
// - Applying state transitions to Storage
// - Maintaining metrics
// - Producing and restoring snapshots
// - Enforcing deterministic application order
//
// Storage: The underlying KV backend.
// Metrics: Runtime statistics for monitoring/telemetry.
//
type FSM struct {
    Storage Storage
    Metrics *Metrics

    LastApplied uint64
}
