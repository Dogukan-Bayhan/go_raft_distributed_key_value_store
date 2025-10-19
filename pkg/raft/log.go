package raft

type EntryType uint8

const (
	EntryNormal EntryType = iota
	EntryConfChange
	EntryBarrier
)


type LogEntry struct {
	Term uint64
	Index uint64
	Type EntryType
	Data []byte
}

func (n *Node) LastLogIndex() uint64 { 
	return uint64(len(n.Log) - 1) 
}


func (n *Node) LastLogTerm() uint64  { 
	return n.Log[len(n.Log)-1].Term 
}

func (n *Node) TermAt(i uint64) (uint64, bool) {
	if i >= uint64(len(n.Log)) {
		return 0, false
	}
	return n.Log[i].Term, true
}