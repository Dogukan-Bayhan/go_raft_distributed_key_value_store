package raft


import (

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


// 
type AppendEntries struct {
	Term uint64
	LeaderID int
	LastLogTerm uint64
	LastLogIndex uint64
	Entries [][]byte
	LeaderCommit uint64
}

type AppendEntriesResponse struct {
	Term uint64
	Success bool
	LastIndex uint64
}