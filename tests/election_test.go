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

    ids := []int{1, 2, 3}
    nodes := make([]*raft.Node, 0, len(ids))
    for _, id := range ids {
        n, err := raft.NewNode(id, ids, router)
        if err != nil {
            t.Fatalf("failed to create node %d: %v", id, err)
        }
        nodes = append(nodes, n)
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