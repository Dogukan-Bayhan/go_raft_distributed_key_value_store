// raft/router.go
package raft

import "sync"

type LocalRouter struct {
	mu    sync.RWMutex
	inbox map[int]chan RpcMessage
}

func NewLocalRouter() *LocalRouter {
	return &LocalRouter{inbox: make(map[int]chan RpcMessage)}
}

func (r *LocalRouter) Register(n *Node) {
	r.mu.Lock()
	r.inbox[n.Id] = n.Inbox
	r.mu.Unlock()
}

func (r *LocalRouter) Send(m RpcMessage) {
	r.mu.RLock()
	ch := r.inbox[m.To]
	r.mu.RUnlock()
	if ch != nil {
		ch <- m
	}
}
