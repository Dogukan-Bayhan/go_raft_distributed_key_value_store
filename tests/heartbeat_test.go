package raft_test

import (
	"context"
	"testing"
	"time"

	"raft/pkg/raft"
)

func TestHeartbeatPreventsElection(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	router := raft.NewLocalRouter()
	nodes := []*raft.Node{
		raft.NewNode(1, []int{1, 2}, router),
		raft.NewNode(2, []int{1, 2}, router),
	}

	for _, n := range nodes {
		router.Register(n)
	}

	for _, n := range nodes {
		go n.Run(ctx)
	}

	
	time.Sleep(2 * time.Second)
	var leader, follower *raft.Node
	for _, n := range nodes {
		if n.Role == raft.Leader {
			leader = n
		} else {
			follower = n
		}
	}

	if leader == nil || follower == nil {
		t.Fatalf("expected 1 leader and 1 follower")
	}

	time.Sleep(2 * time.Second)

	if follower.Role != raft.Follower {
		t.Fatalf("expected follower to stay follower, got %v", follower.Role)
	}
}
