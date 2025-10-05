package omnipaxos

//
// This is an outline of the API that OmniPaxos must expose to
// the service (or tester). See comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   Create a new OmniPaxos server.
// op.Start(command interface{}) (index, ballot, isleader)
//   Start agreement on a new log entry
// op.GetState() (ballot, isLeader)
//   ask a OmniPaxos for its current ballot, and whether it thinks it is leader
// ApplyMsg
//   Each time a new entry is committed to the log, each OmniPaxos peer
//   should send an ApplyMsg to the service (or tester) in the same server.
//

import (
	"sync"
	"sync/atomic"

	"omnipaxos/labrpc"
)

// A Go object implementing a single OmniPaxos peer.
type OmniPaxos struct {
	mu            sync.Mutex          // Lock to protect shared access to this peer's state
	peers         []*labrpc.ClientEnd // RPC end points of all peers
	persister     *Persister          // Object to hold this peer's persisted state
	me            int                 // This peer's index into peers[]
	dead          int32               // Set by Kill()
	enableLogging int32
	// Your code here (3, 4).

}

// As each OmniPaxos peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). Set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// GetState Return the current leader's ballot and whether this server
// believes it is the leader.
func (op *OmniPaxos) GetState() (int, bool) {
	var ballot int
	var isleader bool

	// Your code here (3).

	return ballot, isleader
}

// Called by the tester to submit a log to your OmniPaxos server
// Implement this as described in Figure 3
func (op *OmniPaxos) Proposal(command interface{}) (int, int, bool) {
	index := -1
	ballot := -1
	isLeader := false

	// Your code here (A4).

	return index, ballot, isLeader
}

// The service using OmniPaxos (e.g. a k/v server) wants to start
// agreement on the next command to be appended to OmniPaxos's log. If this
// server isn't the leader, returns false. Otherwise start the
// agreement and return immediately. There is no guarantee that this
// command will ever be committed to the OmniPaxos log, since the leader
// may fail or lose an election. Even if the OmniPaxos instance has been killed,
// this function should return gracefully.
//
// The first return value is the index that the command will appear at
// if it's ever committed. The second return value is the current
// ballot. The third return value is true if this server believes it is
// the leader.

// The tester doesn't halt goroutines created by OmniPaxos after each test,
// but it does call the Kill() method. Your code can use killed() to
// check whether Kill() has been called. The use of atomic avoids the
// need for a lock.
//
// The issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. Any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (op *OmniPaxos) Kill() {
	atomic.StoreInt32(&op.dead, 1)
	// Your code here, if desired.
	// you may set a variable to false to
	// disable logs as soon as a server is killed
	atomic.StoreInt32(&op.enableLogging, 0)
}

func (op *OmniPaxos) killed() bool {
	z := atomic.LoadInt32(&op.dead)
	return z == 1
}

// The service or tester wants to create a OmniPaxos server. The ports
// of all the OmniPaxos servers (including this one) are in peers[]. This
// server's port is peers[me]. All the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects OmniPaxos to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *OmniPaxos {

	op := &OmniPaxos{}
	op.peers = peers
	op.persister = persister
	op.me = me

	// Your initialization code here (3, 4).

	return op
}
