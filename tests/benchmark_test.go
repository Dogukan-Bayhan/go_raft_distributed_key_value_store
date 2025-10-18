package raft_test

import (
	"context"
	"raft/pkg/raft"
	"testing"
	"time"
)

func BenchmarkElection(b *testing.B) {
	for i := 0; i < b.N; i++ {
		ctx, cancel := context.WithCancel(context.Background())
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

		start := time.Now()
		for {
			leaders := 0
			for _, n := range nodes {
				if n.Role == raft.Leader {
					leaders++
				}
			}
			if leaders == 1 {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		cancel()
		b.Logf("Election took %v", time.Since(start))
	}
}
