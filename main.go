package main

import (
	"context"
	"log"
	"raft/pkg/raft"
	"sync/atomic"
	"time"

)

func main() {
	log.Println("=== Starting Raft Testtttttt ===")

	router := raft.NewLocalRouter()
	peerIDs := []int{1, 2, 3}
	nodes := make([]*raft.Node, 0, len(peerIDs))

	// Create 3 nodes
	for _, id := range peerIDs {
		n := raft.NewNode(id, peerIDs, router)
		router.Register(n)
		nodes = append(nodes, n)
		ctx := context.Background()
		go n.Run(ctx)
	}

	// Wait for elections
	time.Sleep(2 * time.Second)

	// Find the leader
	var leader *raft.Node
	for _, n := range nodes {
		if n.Role == raft.Leader {
			leader = n
			break
		}
	}

	if leader == nil {
		log.Fatal("Don't find the leader, election failed!")
	}
	log.Printf("Leader chose: n%d (term=%d)", leader.Id, atomic.LoadUint64(&leader.CurrentTerm))

	// Client -> Leader log
	entry := raft.LogEntry{
		Term:  atomic.LoadUint64(&leader.CurrentTerm),
		Index: leader.LastLogIndex() + 1,
		Type:  raft.EntryNormal,
		Data:  []byte("SET x=42"),
	}
	leader.Log = append(leader.Log, entry)
	leader.LastIndex = entry.Index
	leader.LastTerm = entry.Term

	log.Printf("Client send the log: %s", entry.Data)

	// Lider, replicate the log to followers
	for _, peer := range leader.Peers {
		if peer == leader.Id {
			continue
		}
		go leader.ReplicateTo(peer)
	}

	// Wait for replication
	time.Sleep(2 * time.Second)

	log.Println("Log states:")
	for _, n := range nodes {
		log.Printf("Node %d | Role=%v | LogLen=%d", n.Id, n.Role, len(n.Log))
		for i, e := range n.Log {
			log.Printf("  [%d] term=%d data=%s", i, e.Term, string(e.Data))
		}
	}

	// Validations
	expectedLen := len(leader.Log)
	allMatch := true
	for _, n := range nodes {
		if len(n.Log) != expectedLen {
			allMatch = false
		}
	}

	if allMatch {
		log.Println("All nodes have the same log content — replication successful!")
	} else {
		log.Println("Some nodes have missing or incompatible logs.")
	}

	// Programı bir süre açık tut
	time.Sleep(1 * time.Second)
	log.Println("=== Test completeddd ===")
}
