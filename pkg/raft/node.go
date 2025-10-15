package raft

import (
	"math/rand"
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

	LastTerm uint64
	LastIndex uint64

	// Voted for is store the last voted candidate id
	VotedFor int

	// Peers id list
	Peers []int
	Role Role

	Inbox chan RpcMessage
	ApplyChan chan []byte
	StopChan chan struct{}

	ElectionTimer *time.Timer
	HeartbeatTicker *time.Ticker

}

func NewNode(id int, peers []int) *Node{
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