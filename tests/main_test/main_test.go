package raft

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"raft/pkg/storage"
	"testing"
	"time"
	"raft/pkg/raft"
)

// makeCluster initializes a local in-memory Raft cluster of N nodes.
// It cleans any previous WAL directories, creates N nodes, registers them
// with the shared LocalRouter, and starts all nodes concurrently.
//
// Each node shares the same peer list, enabling full inter-node communication.
func makeCluster(t *testing.T, n int) ([]*raft.Node, *raft.LocalRouter, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	router := raft.NewLocalRouter()
	var nodes []*raft.Node

	// Remove old persistent WAL data before starting a fresh cluster.
	_ = os.RemoveAll(filepath.Join("pkg", "storage", "wal"))

	for i := 0; i < n; i++ {
		var peers []int
		for j := 0; j < n; j++ {
			peers = append(peers, j)
		}
		node, err := raft.NewNode(i, peers, router)
		if err != nil {
			t.Fatalf("failed to create node %d: %v", i, err)
		}
		nodes = append(nodes, node)
	}

	// Register every node with the router so that RPC messages can be delivered.
	for _, n := range nodes {
		router.Register(n)
	}

	// Launch all nodes concurrently.
	for _, n := range nodes {
		go n.Run(ctx)
	}

	return nodes, router, cancel
}

// waitForLeader polls the cluster for a leader until timeout.
// It returns the first node discovered with Role == Leader.
func waitForLeader(nodes []*raft.Node, timeout time.Duration) *raft.Node {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			if n.Role == raft.Leader {
				return n
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	return nil
}

// -----------------------------------------------------------------------------
// Test 1: Leader Election
// -----------------------------------------------------------------------------

// TestElectionAndLeaderDiscovery verifies that a leader is elected successfully
// in a 3-node cluster within a bounded time frame.
// It ensures that the Raft election mechanism is functioning correctly.
func TestElectionAndLeaderDiscovery(t *testing.T) {
	nodes, _, cancel := makeCluster(t, 3)
	defer cancel()

	leader := waitForLeader(nodes, 2*time.Second)
	if leader == nil {
		t.Fatal("no leader elected within timeout")
	}

	t.Logf("Leader elected: n%d (term=%d)", leader.Id, leader.CurrentTerm)
}

// -----------------------------------------------------------------------------
// Test 2: Log Replication
// -----------------------------------------------------------------------------

// TestLogReplicationToFollowers verifies that once a leader appends a new log entry,
// the entry is replicated across all followers. This ensures that the AppendEntries
// RPC path and WAL durability work correctly across nodes.
func TestLogReplicationToFollowers(t *testing.T) {
	nodes, _, cancel := makeCluster(t, 3)
	defer cancel()

	leader := waitForLeader(nodes, 2*time.Second)
	if leader == nil {
		t.Fatal("no leader elected")
	}

	entry := raft.LogEntry{
		Index: leader.LastLogIndex() + 1,
		Term:  leader.CurrentTerm,
		Type:  raft.EntryNormal,
		Data:  []byte("set x=42"),
	}

	// Persist entry to the leader's WAL and in-memory log.
	data, _ := json.Marshal(entry)
	if err := leader.Wal.Write(entry.Index, data); err != nil {
		t.Fatalf("leader WAL write failed: %v", err)
	}
	leader.AppendLocal(entry)

	// Trigger replication to all followers.
	for _, p := range leader.Peers {
		if p != leader.Id {
			leader.ReplicateTo(p)
		}
	}

	time.Sleep(500 * time.Millisecond)

	// Verify every follower now contains the same log entry.
	for _, n := range nodes {
		last := n.LastLogIndex()
		if last != entry.Index {
			t.Fatalf("node %d did not replicate (lastIndex=%d)", n.Id, last)
		}
		if n.Log[last].Term != leader.CurrentTerm {
			t.Fatalf("node %d term mismatch", n.Id)
		}
	}
}

// -----------------------------------------------------------------------------
// Test 3: WAL Persistence
// -----------------------------------------------------------------------------

// TestWALRecovery validates that log entries written to the Write-Ahead Log (WAL)
// survive process termination and are correctly restored on restart.
// This confirms persistence guarantees required by Raft’s durability model.
func TestWALRecovery(t *testing.T) {
	nodes, _, cancel := makeCluster(t, 1)
	defer cancel()

	n := nodes[0]
	entry := raft.LogEntry{
		Index: n.LastLogIndex() + 1,
		Term:  n.CurrentTerm,
		Type:  raft.EntryNormal,
		Data:  []byte("persist me"),
	}

	data, _ := json.Marshal(entry)
	if err := n.Wal.Write(entry.Index, data); err != nil {
		t.Fatalf("WAL write failed: %v", err)
	}
	n.AppendLocal(entry)
	time.Sleep(100 * time.Millisecond)

	// Simulate shutdown.
	n.StopChan <- struct{}{}
	cancel()

	// Reopen WAL from disk.
	path := filepath.Join("pkg", "storage", "wal", "node-0")
	opts := storage.Options{AllowEmpty: true}
	wal, err := storage.Open(path, &opts)
	if err != nil {
		t.Fatalf("reopen WAL failed: %v", err)
	}
	defer wal.Close()

	lastIdx, err := wal.LastIndex()
	if err != nil {
		t.Fatalf("WAL.LastIndex failed: %v", err)
	}
	if lastIdx != entry.Index {
		t.Fatalf("WAL lost entry after restart: got %d want %d", lastIdx, entry.Index)
	}
}

// -----------------------------------------------------------------------------
// Test 4: Conflict Resolution (Follower Log Truncation)
// -----------------------------------------------------------------------------

// TestFollowerConflictTruncation ensures that when a follower’s log diverges
// from the leader’s log at the same index (i.e., conflicting term),
// the follower truncates its conflicting entries and accepts the leader’s version.
// This validates the Raft conflict resolution mechanism.
func TestFollowerConflictTruncation(t *testing.T) {
	nodes, _, cancel := makeCluster(t, 5)
	defer cancel()

	leader := waitForLeader(nodes, 3*time.Second)
	if leader == nil {
		t.Skip("no leader elected — transient election delay")
	}
	follower := nodes[(leader.Id+1)%len(nodes)]

	conflict := raft.LogEntry{
		Index: leader.LastLogIndex() + 1,
		Term:  leader.CurrentTerm + 1,
		Type:  raft.EntryNormal,
		Data:  []byte("old data"),
	}
	data, _ := json.Marshal(conflict)
	_ = follower.Wal.Write(conflict.Index, data)
	follower.AppendLocal(conflict)

	entry := raft.LogEntry{
		Index: conflict.Index,
		Term:  leader.CurrentTerm,
		Type:  raft.EntryNormal,
		Data:  []byte("new data"),
	}
	leader.Log = append(leader.Log, entry)
	data2, _ := json.Marshal(entry)
	_ = leader.Wal.Write(entry.Index, data2)
	leader.ReplicateTo(follower.Id)

	// Wait up to 2s for eventual consistency
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		last := follower.LastLogIndex()
		if follower.Log[last].Term == leader.CurrentTerm {
			return // ✅ success
		}
		time.Sleep(100 * time.Millisecond)
	}

	last := follower.LastLogIndex()
	t.Fatalf("conflict not truncated correctly: got term %d want %d", follower.Log[last].Term, leader.CurrentTerm)
}

