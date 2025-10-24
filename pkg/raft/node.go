package raft

import (
	"context"
	"log"
	"math/rand"
	"sync/atomic"
	"time"
)

// This are the roles of the raft server nodes
// There are 3 different roles which are Follower Candidate and Leader
type Role uint8

const (
	Follower Role = iota
	Candidate
	Leader
)

// Vote request is used for rpc that send from Candidate to Followers
// When a candidate wants to be a Leader they send Vote Requests to Followers to take enough vote
type VoteRequest struct {
	LastTerm    uint64 // Last term is the last term of the candidates log file
	LastIndex   uint64 // Last index is the last index of the candidates log file
	CandidateId int
	// After candidate start voting they increase their term. Thats why we use both Last Term and Term
	Term uint64
}

type VoteResponse struct {
	Term    uint64
	IsVoted bool
}

// This is struct is use for these rpcs:
// 1-> General heartbeat
// 2-> Commands that comes from client with entries
// 3-> Catch up message
type AppendEntries struct {
	Term         uint64 // Leaders Term
	LeaderID     int
	LastLogTerm  uint64
	LastLogIndex uint64
	Entries      []LogEntry
	LeaderCommit uint64 // Leaders commit index
}

type AppendEntriesResponse struct {
	Term      uint64
	Success   bool
	LastIndex uint64 // Candidates commit index
}

// Now this is a general message format
type RpcMessage struct {
	From int
	To   int
	Body any
}

// Base raft servers Node structure
type Node struct {
	Id int

	// Current term is used for current leadership term
	// so this is important for elections
	CurrentTerm uint64

	// These two members are last occurence in the
	// Node's log file
	LastTerm  uint64
	LastIndex uint64

	// Voted for is store the last voted candidate id
	VotedFor     int
	Votes        int
	ElectionTerm uint64

	// Peers id list
	Peers []int
	Role  Role

	Log         []LogEntry
	// Last applied is the index number that 
	// the last occurence inde in the log entry
	// LastApplied must <= CommitIndex
	LastApplied uint64

	// These are used by only leader
	NextIndex  map[int]uint64 
	MatchIndex map[int]uint64

	CommitIndex uint64

	Inbox     chan RpcMessage
	ApplyChan chan []byte
	StopChan  chan struct{}

	ElectionTimer   *time.Timer
	HeartbeatTicker *time.Ticker

	Router  *LocalRouter
	Pending map[uint64]chan []byte
}

func NewNode(id int, peers []int, r *LocalRouter) *Node {
	rand.Seed(time.Now().UnixNano())
	n := &Node{
		Id:              id,
		Peers:           peers,
		Role:            Follower,
		VotedFor:        -1,
		Inbox:           make(chan RpcMessage, 1024),
		ApplyChan:       make(chan []byte, 1024),
		StopChan:        make(chan struct{}),
		ElectionTimer:   time.NewTimer(jitterElection()),
		HeartbeatTicker: nil,
		Router:          r,
		Log:             []LogEntry{{Term: 0, Index: 0, Type: EntryBarrier}},
		LastApplied:     0,
		LastIndex:       0,
		LastTerm:        0,
		Pending:         make(map[uint64]chan []byte),
	}

	return n
}

// jitterElection returns a randomized election timeout between 300ms and 600ms.
// Raft uses this randomization so that followers do not start elections
// at the same time (avoiding split votes). Each node gets a slightly different
// timeout duration to improve cluster stability.
// Also you change duration multiplyer for your own project
func jitterElection() time.Duration {
	durationMultiplyer := 300

	return time.Duration(durationMultiplyer)*time.Millisecond + time.Duration(rand.Intn(durationMultiplyer))*time.Millisecond
}

func (n *Node) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-n.StopChan:
			return
		case <-n.ElectionTimer.C:
			n.startElection()
		case m := <-n.Inbox:
			n.handleRPC(m)
		case <-n.heartbeatTick():
			if n.Role == Leader {
				n.broadcastHeartbeats()
			}
		}
	}
}

// This is the timer that used by leader to send a heartbeat to the 
// followers or candidates to tell them leader Node is still alive
func (n *Node) heartbeatTick() <-chan time.Time {
	if n.HeartbeatTicker == nil {

		return nil
	}

	return n.HeartbeatTicker.C
}


// If the timer of the Node is send message Nodes try to start
// an election and request vote from all peers
func (n *Node) startElection() {
	// If this Node is still leader reset the election timer
	if n.Role == Leader {
		n.resetElection()
		return
	}

	term := atomic.AddUint64(&n.CurrentTerm, 1)
	n.Role = Candidate
	n.VotedFor = n.Id

	n.ElectionTerm = term
	n.Votes = 1

	log.Printf("[n%d] start a new election for term=%d", n.Id, term)

	currentTerm, currentIndex := n.LastLogTerm(), n.LastLogIndex()

	// In here form term Node uses its CurrentTerm + 1
	req := VoteRequest{
		Term:        term,
		LastTerm:    currentTerm,
		LastIndex:   currentIndex,
		CandidateId: n.Id,
	}

	// Request all peers in its peer list
	for _, p := range n.Peers {
		if p == n.Id {
			continue
		}
		n.sendRPC(RpcMessage{From: n.Id, To: p, Body: req})
	}

	// votes >= floor(N/2) + 1

	n.resetElection()
}


// If candidate have enough vote set itself as a leader
func (n *Node) becomeLeader() {
	n.Role = Leader

	// Close its timer if this timer is not nil than 
	// after a time leader start a new election for itself
	if n.ElectionTimer != nil {
		if !n.ElectionTimer.Stop() {
			select {
			case <-n.ElectionTimer.C:
			default:
			}
		}
	}

	// After every election leader set index to log replication
	// Next Index is starting from last log index + 1 and 
	// decreased by 1 if followers logs index is lesser than the 
	// leaders log index
	n.NextIndex = make(map[int]uint64, len(n.Peers))
	n.MatchIndex = make(map[int]uint64, len(n.Peers))
	li := n.LastLogIndex()
	for _, p := range n.Peers {
		n.NextIndex[p] = li + 1
		n.MatchIndex[p] = 0
	}
	n.MatchIndex[n.Id] = li

	// I chose a 80 miliseconds for heartbeat messages because its enough
	// for my project but this depends on heartbeat round-trip time and also
	// election timouts.

	// Generaly if servers are in the same data center than RTT is nearly 0.1 to 2 ms
	// So 80 ms is fearly enough for my project because I don't have different servers
	// So I run this servers in my computers different ports.
	n.HeartbeatTicker = time.NewTicker(80 * time.Millisecond)
	log.Printf("[n%d] became LEADER term=%d", n.Id, atomic.LoadUint64(&n.CurrentTerm))
}

// This function is a general function that set the nodes role as follower
// It uses when a valid leader send a heartbeat or append an entrie or a candidate request a vote which term and index is enough
func (n *Node) becomeFollower(term uint64, votedFor int) {
	atomic.StoreUint64(&n.CurrentTerm, term)
	n.Role = Follower
	n.VotedFor = votedFor
	if n.HeartbeatTicker != nil {
		n.HeartbeatTicker.Stop()
		n.HeartbeatTicker = nil
	}
	n.resetElection()
}

// Reset the election timer with a random value between 300ms and 600ms
func (n *Node) resetElection() {
	if !n.ElectionTimer.Stop() {
		select {
		case <-n.ElectionTimer.C:
		default:
		}
	}
	n.ElectionTimer.Reset(jitterElection())
}


// 
func (n *Node) handleRPC(m RpcMessage) {
	switch msg := m.Body.(type) {
	case AppendEntries:
		n.handleAppendEntries(m.From, msg)
	case VoteRequest:
		n.handleRequestVote(m.From, msg)
	case VoteResponse:
		n.handleVoteResponse(m.From, msg)
	case AppendEntriesResponse:
		n.handleAppendEntriesResponse(m.From, msg)
	default:
		// ignore
	}
}

// After vote request if followers term is greater than 
// candidates term candidate becomes follower again
// If candidate is suitable to become leader and also
// vote is masjority then Node become a leader
func (n *Node) handleVoteResponse(from int, resp VoteResponse) {

	term := atomic.LoadUint64(&n.CurrentTerm)

	if resp.Term > term {
		n.becomeFollower(resp.Term, -1)
		return
	}

	if n.Role != Candidate || resp.Term != n.ElectionTerm {
		return
	}

	if resp.IsVoted {
		n.Votes++
		majority := (len(n.Peers) / 2) + 1

		if n.Votes >= majority {
			n.becomeLeader()
		}
	}
}

// This function called either for heartbeat and also log replication
// If entires is empty then this is a heartbeat
// If entries is not empty that means this node replicate the given log entries in its log file
func (n *Node) handleAppendEntries(from int, req AppendEntries) {

	// If the term of the node is greater than the leader that means leader is an old leader so send an RPC with succes: false value
	term := atomic.LoadUint64(&n.CurrentTerm)
	if req.Term < term {

		n.sendRPC(RpcMessage{From: n.Id, To: from, Body: AppendEntriesResponse{Term: term, Success: false, LastIndex: n.LastLogIndex()}})
		return
	}

	// If the leader is valid, then resest the election timer and also set role as follower
	if req.Term > term || n.Role != Follower {
		n.becomeFollower(req.Term, -1)
	}
	n.resetElection()

	if req.LastLogIndex > n.LastLogIndex() {
		n.sendRPC(RpcMessage{From: n.Id, To: from, Body: AppendEntriesResponse{
			Term: atomic.LoadUint64(&n.CurrentTerm), Success: false, LastIndex: n.LastLogIndex(),
		}})
		return
	}

	if req.LastLogIndex > 0 {
		t, ok := n.TermAt(req.LastLogIndex)
		if !ok || t != req.LastLogTerm {
			n.Log = n.Log[:req.LastLogIndex+1]
			n.LastIndex, n.LastTerm = n.LastLogIndex(), n.LastLogTerm()
			n.sendRPC(RpcMessage{From: n.Id, To: from, Body: AppendEntriesResponse{
				Term: atomic.LoadUint64(&n.CurrentTerm), Success: false, LastIndex: n.LastLogIndex(),
			}})
			return
		}
	}

	if len(req.Entries) > 0 {
		insertAt := req.LastLogIndex + 1
		if insertAt <= n.LastLogIndex() {
			n.Log = n.Log[:insertAt]
		}
		n.Log = append(n.Log, req.Entries...)
		n.LastIndex, n.LastTerm = n.LastLogIndex(), n.LastLogTerm()
	}

	if req.LeaderCommit > n.CommitIndex {
		hi := n.LastLogIndex()
		if req.LeaderCommit < hi {
			hi = req.LeaderCommit
		}
		for idx := n.CommitIndex + 1; idx <= hi; idx++ {
			e := n.Log[idx]
			if e.Type == EntryNormal && n.ApplyChan != nil {
				n.ApplyChan <- e.Data
			}
			n.LastApplied = idx
		}
		n.CommitIndex = hi
	}

	n.sendRPC(RpcMessage{From: n.Id, To: from, Body: AppendEntriesResponse{
		Term:      atomic.LoadUint64(&n.CurrentTerm),
		Success:   true,
		LastIndex: 0,
	}})
}

func (n *Node) handleAppendEntriesResponse(from int, resp AppendEntriesResponse) {
	if n.Role != Leader {
		return
	}

	myTerm := atomic.LoadUint64(&n.CurrentTerm)
	if resp.Term > myTerm {
		n.becomeFollower(resp.Term, -1)
		return
	}

	if !resp.Success {
		if n.NextIndex[from] > 1 {
			n.NextIndex[from]--
		}
		n.ReplicateTo(from)
		return
	}

	if resp.LastIndex > n.MatchIndex[from] {
		n.MatchIndex[from] = resp.LastIndex
	}
	n.NextIndex[from] = n.MatchIndex[from] + 1

	n.maybeAdvanceCommit()
}

func (n *Node) maybeAdvanceCommit() {
	if n.Role != Leader {
		return
	}
	oldCommit := n.CommitIndex
	last := n.LastLogIndex()
	for N := oldCommit + 1; N <= last; N++ {
		if n.Log[N].Term != n.CurrentTerm {
			continue
		}
		count := 1
		for _, p := range n.Peers {
			if p == n.Id {
				continue
			}
			if n.MatchIndex[p] >= N {
				count++
			}
		}
		if count >= (len(n.Peers)/2)+1 {
			n.CommitIndex = N
		}
	}
	if n.CommitIndex != oldCommit {
		n.applyCommitted()
	}
}

func (n *Node) applyCommitted() {
	for n.LastApplied < n.CommitIndex {
		n.LastApplied++
		e := n.Log[n.LastApplied]

		if e.Type == EntryNormal && n.ApplyChan != nil {
			n.ApplyChan <- e.Data
		}

		if w, ok := n.Pending[n.LastApplied]; ok {
			select {
			case w <- e.Data:
			default:
			}
			close(w)
			delete(n.Pending, n.LastApplied)
		}
	}
}

func (n *Node) handleRequestVote(from int, req VoteRequest) {
	term := atomic.LoadUint64(&n.CurrentTerm)
	if req.Term < term {
		n.sendRPC(RpcMessage{From: n.Id, To: from, Body: VoteResponse{Term: term, IsVoted: false}})
		return
	}

	if req.Term > term {
		n.becomeFollower(req.Term, -1)
	}

	myLastIdx, myLastTerm := n.LastLogIndex(), n.LastLogTerm()
	upToDate := (req.LastTerm > myLastTerm) || (req.LastTerm == myLastTerm && req.LastIndex >= myLastIdx)

	grant := false
	if upToDate && (n.VotedFor == -1 || n.VotedFor == req.CandidateId) && n.Role != Leader {
		grant = true
		n.VotedFor = req.CandidateId
		n.resetElection()
	}
	log.Println(grant)
	n.sendRPC(RpcMessage{
		From: n.Id, To: from,
		Body: VoteResponse{Term: atomic.LoadUint64(&n.CurrentTerm), IsVoted: grant},
	})
}

func (n *Node) broadcastHeartbeats() {
	term := atomic.LoadUint64(&n.CurrentTerm)
	for _, p := range n.Peers {
		if p == n.Id {
			continue
		}

		prevIdx := n.NextIndex[p] - 1
		prevTerm, _ := n.TermAt(prevIdx)

		req := AppendEntries{
			Term:         term,
			LeaderID:     n.Id,
			LastLogIndex: prevIdx,
			LastLogTerm:  prevTerm,
			Entries:      nil,
			LeaderCommit: n.CommitIndex,
		}

		n.sendRPC(RpcMessage{From: n.Id, To: p, Body: req})
	}
}

func (n *Node) sendRPC(m RpcMessage) {
	if n.Router != nil {
		n.Router.Send(m)
	}
}

func (n *Node) AppendLocal(e LogEntry) {
	n.Log = append(n.Log, e)
	n.LastIndex = e.Index
	n.LastTerm = e.Term
}

func (n *Node) ReplicateTo(peer int) {
	if n.Role != Leader {
		return
	}

	next := n.NextIndex[peer]
	prevIdx := next - 1
	prevTerm, _ := n.TermAt(prevIdx)

	last := n.LastLogIndex()
	var entries []LogEntry
	if next <= last {
		entries = make([]LogEntry, last-next+1)
		copy(entries, n.Log[next:])
	} else {
		entries = nil
	}

	req := AppendEntries{
		Term:         n.CurrentTerm,
		LeaderID:     n.Id,
		LastLogIndex: prevIdx,
		LastLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: n.CommitIndex,
	}
	n.sendRPC(RpcMessage{From: n.Id, To: peer, Body: req})
}

