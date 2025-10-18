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
type VoteRequest struct{
	LastTerm uint64 // Last term is the last term of the candidates log file
	LastIndex uint64 // Last index is the last index of the candidates log file
	CandidateId int 
	// After candidate start voting they increase their term. Thats why we use both Last Term and Term
	Term uint64 
}

type VoteResponse struct {
	Term uint64
	IsVoted bool
}


// This is struct is use for these rpcs:
// 1-> General heartbeat
// 2-> Commands that comes from client with entries
// 3-> Catch up message 
type AppendEntries struct {
	Term uint64 // Leaders Term
	LeaderID int 
	LastLogTerm uint64 
	LastLogIndex uint64
	Entries [][]byte
	LeaderCommit uint64 // Leaders commit index
}

type AppendEntriesResponse struct {
	Term uint64
	Success bool
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

	CurrentTerm uint64
	LastTerm uint64
	LastIndex uint64

	// Voted for is store the last voted candidate id
	VotedFor int
	Votes        int
    ElectionTerm uint64

	// Peers id list
	Peers []int
	Role Role


	CommitIndex uint64

	Inbox chan RpcMessage
	ApplyChan chan []byte
	StopChan chan struct{}

	ElectionTimer *time.Timer
	HeartbeatTicker *time.Ticker

	Router *LocalRouter

}

func NewNode(id int, peers []int, r*LocalRouter) *Node{
	rand.Seed(time.Now().UnixNano())
	n := &Node{
		Id: id,
		Peers: peers,
		Role: Follower,
		VotedFor: -1,
		Inbox: make(chan RpcMessage, 1024),
		ApplyChan: make(chan []byte, 1024),
		StopChan: make(chan struct{}),
		ElectionTimer: time.NewTimer(jitterElection()),
		HeartbeatTicker: nil,
		Router: r,
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
		case <- n.StopChan:
			return
		case <- n.ElectionTimer.C:
			n.startElection()
		case m := <- n.Inbox:
			n.handleRPC(m)
		case <- n.heartbeatTick():
			if n.Role == Leader {
				n.broadcastHeartbeats()
			}
		}
	}
}

func (n *Node) heartbeatTick() <- chan time.Time {
	if n.HeartbeatTicker == nil {

		return nil
	}

	return n.HeartbeatTicker.C
}

func (n *Node) startElection() {
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

	req := VoteRequest{
		Term: term,
		LastTerm: n.CurrentTerm,
		LastIndex: n.LastIndex,
		CandidateId: n.Id,
	}

	for _, p := range n.Peers {
		if p == n.Id {
			continue
		}
		n.sendRPC(RpcMessage{From: n.Id, To: p, Body: req})
	}

	// votes >= floor(N/2) + 1

	n.resetElection()
}

func (n * Node) becomeLeader() {
	n.Role = Leader

	if n.ElectionTimer != nil {
        if !n.ElectionTimer.Stop() {
            select { case <-n.ElectionTimer.C: default: }
        }
    }

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
		select { case <-n.ElectionTimer.C: default: }
	}
	n.ElectionTimer.Reset(jitterElection())
}

func (n *Node) handleRPC(m RpcMessage) {
	switch msg := m.Body.(type) {
	case AppendEntries:
		n.handleAppendEntries(m.From, msg)
	case VoteRequest:
		n.handleRequestVote(m.From, msg)
	case VoteResponse:
		n.handleVoteResponse(m.From, msg)
	default:
		// ignore
	}
}

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
		
		n.sendRPC(RpcMessage{From: n.Id, To: from, Body: AppendEntriesResponse{Term: term, Success: false}})
		return
	}
	

	// If the leader is valid, then resest the election timer and also set role as follower
	if req.Term > term || n.Role != Follower {
		n.becomeFollower(req.Term, -1)
	}
	n.resetElection()

	// In here we normally sync the log with given body
	// logSync()

	// Send a success rpc to leader
	n.sendRPC(RpcMessage{From: n.Id, To: from, Body: AppendEntriesResponse{
		Term:      atomic.LoadUint64(&n.CurrentTerm),
		Success:   true,
		LastIndex: 0,
	}})
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
	grant := false
	if (n.VotedFor == -1 || n.VotedFor == req.CandidateId) && n.Role != Leader {
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
	req := AppendEntries{Term: term, LeaderID: n.Id}
	for _, p := range n.Peers {
		if p == n.Id {
			continue
		}
		n.sendRPC(RpcMessage{From: n.Id, To: p, Body: req})
	}
}

func (n *Node) sendRPC(m RpcMessage) {
	if n.Router != nil {
		n.Router.Send(m)
	}

	// İlk aşama: aynı proses içinde kanal ile (test kolaylığı)
	// Gerçek ağ sürümünde: net.Dial -> gob/json ile m.Body encode et.
	// Bu iskelette boş bırakıyoruz; test harness dolduracak.
}