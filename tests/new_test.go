package raft_test

import (
	"context"
	"testing"
	"time"

	"raft/pkg/raft"
)

func TestCrashRecoveryElection(t *testing.T) {
	router := raft.NewLocalRouter()

	nodes := []*raft.Node{
		raft.NewNode(1, []int{1, 2, 3, 4, 5}, router),
		raft.NewNode(2, []int{1, 2, 3, 4, 5}, router),
		raft.NewNode(3, []int{1, 2, 3, 4, 5}, router),
		raft.NewNode(4, []int{1, 2, 3, 4, 5}, router),
		raft.NewNode(5, []int{1, 2, 3, 4, 5}, router),
	}

	for _, n := range nodes {
		router.Register(n)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for _, n := range nodes {
		go n.Run(ctx)
	}

	findLeader := func(minTerm uint64) *raft.Node {
		for i := 0; i < 100; i++ { 
			for _, n := range nodes {
				if n.Role == raft.Leader && n.CurrentTerm > minTerm {
					return n
				}
			}
			time.Sleep(100 * time.Millisecond)
		}
		return nil
	}

	
	leader := findLeader(0)
	if leader == nil {
		t.Fatalf("no leader elected initially")
	}

	t.Logf("Initial Leader: n%d (term=%d)", leader.Id, leader.CurrentTerm)
	lastTerm := leader.CurrentTerm

	
	for crashRound := 1; crashRound <= 3; crashRound++ {
		
		t.Logf("Round %d: Crashing Leader n%d (term=%d)", crashRound, leader.Id, leader.CurrentTerm)
		close(leader.StopChan) 

		time.Sleep(400 * time.Millisecond)

		newLeader := findLeader(lastTerm)
		if newLeader == nil {
			t.Fatalf("no new leader elected after crash %d", crashRound)
		}

		t.Logf("After crash %d → New Leader: n%d (term=%d)", crashRound, newLeader.Id, newLeader.CurrentTerm)

		if newLeader.CurrentTerm <= lastTerm {
			t.Fatalf("expected higher term after crash %d (was %d, got %d)", crashRound, lastTerm, newLeader.CurrentTerm)
		}

		lastTerm = newLeader.CurrentTerm
		leader = newLeader
	}
}
