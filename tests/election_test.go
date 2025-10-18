package raft_test

import(
	"context"
	"testing"
	"time"

	"raft/pkg/raft"
)

func TestSingleElection(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	router := raft.NewLocalRouter()

	nodes := []*raft.Node{
        raft.NewNode(1, []int{1, 2, 3}, router),
        raft.NewNode(2, []int{1, 2, 3}, router),
        raft.NewNode(3, []int{1, 2, 3}, router),
    }

    for _, n := range nodes {
        router.Register(n)
    }

    for _, n := range nodes {
        go n.Run(ctx)
    }

	time.Sleep(2 * time.Second)

    leaderCount := 0
    for _, n := range nodes {
        if n.Role == raft.Leader {
            leaderCount++
        }
    }

    if leaderCount != 1 {
        t.Fatalf("expected 1 leader, got %d", leaderCount)
    }
}