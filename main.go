package main

import (
	"context"
	"log"
	"raft/pkg/raft"
	"time"
)

func main() {
	log.SetFlags(log.Lmicroseconds)

	r := raft.NewLocalRouter()
	peers := []int{1, 2, 3}

	n1 := raft.NewNode(1, peers, r)
	n2 := raft.NewNode(2, peers, r)
	n3 := raft.NewNode(3, peers, r)

	r.Register(n1)
	r.Register(n2)
	r.Register(n3)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go n1.Run(ctx)
	go n2.Run(ctx)
	go n3.Run(ctx)

	time.Sleep(2 * time.Second) // lider seçimini bekle

	log.Println(">>> Simulating leader crash (n1 stops)")
	close(n1.StopChan)

	time.Sleep(3 * time.Second) // yeniden seçim bekle
}
