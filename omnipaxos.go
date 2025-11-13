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
	"time"

	"omnipaxos/labrpc"

	"github.com/rs/zerolog/log"
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

	// Persistent state on all servers (OmniPaxos paper Figure 3)
	//log         []LogEntry
	promisedRnd BallotNumber
	acceptedRnd BallotNumber
	decidedIdx  int

	// Volatile state on all servers
	// state ---- the role and phase a server is in. Initially (FOLLOWER, PREPARE)

	// Volatile state of leader
	currentRound BallotNumber
	//.....

	// BLE (Ballot Leader Election, OmniPaxos paper Figure 4)
	// Persistent state on all servers
	l BallotNumber // ballot number of the current leader

	// Volatile state on all servers
	r       int          // current heartbeat round. Initially set to 0
	b       BallotNumber // ballot number. Initially set to (0, pid)
	qc      bool         // quorum-connected flag. Initially set to true
	delay   int          // delay the duration a server waits for heartbeat replies within a single round
	ballots []BallotInfo // set of (ballot, qc) pairs received in the current round
}

type BallotNumber struct {
	Ballot int
	ID     int
}

type BallotInfo struct {
	Ballot BallotNumber
	QC     bool
}

type HBRequestArgs struct {
	Rnd int // the round of this request
}

// Heartbeat reply for BLE (Figure 4)
type HBRequestReply struct {
	Rnd    int          // the round this reply was sent in
	Ballot BallotNumber // ballot number of the server sending the reply
	Q      bool         // quorum-connected flag of the server sending the reply
}

type LeaderRequest struct {
	S int          // the elected server
	N BallotNumber // the round s got elected in
}

type LeaderReply struct {
	// empty
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
	op.mu.Lock()
	defer op.mu.Unlock()
	ballot = op.l.Ballot
	isleader = op.l.ID == op.me

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
func Make(peers []*labrpc.ClientEnd, me int, // peers are all the servers. pees[me] is the server but you won't need to use it.
	persister *Persister, applyCh chan ApplyMsg) *OmniPaxos {

	op := &OmniPaxos{}
	op.peers = peers
	op.persister = persister //
	// op.applyCh
	op.me = me

	// Your initialization code here (3, 4).

	// Initialize BLE state
	op.r = 0
	op.b = BallotNumber{Ballot: 0, ID: op.me}
	op.l = BallotNumber{Ballot: 0, ID: -1} // no leader initially
	op.qc = true
	op.delay = 150
	op.ballots = make([]BallotInfo, 0)

	// BLE heartbeat goroutine
	go op.startTimer()

	return op
}

// Upon receiving ⟨HBRequest⟩ from server s
// Fields:
// rnd --- the round of this request
//
// Receiver implementation:
// Send ⟨HBReply, rnd, b, qc⟩ to s
func (op *OmniPaxos) Heartbeat(args *HBRequestArgs, reply *HBRequestReply) {
	op.mu.Lock()
	// log.Info().Msgf("[%d] Heartbeat called: round=%d, receiver time=%v", op.me, args.Rnd, time.Now())
	defer op.mu.Unlock()

	// Reply with current round, ballot, and quorum-connected flag
	reply.Rnd = args.Rnd
	reply.Ballot = op.b
	reply.Q = op.qc
}

// Helper function to compare ballot numbers
func (b1 BallotNumber) Greater(b2 BallotNumber) bool {
	if b1.Ballot > b2.Ballot {
		return true
	}
	if b1.Ballot == b2.Ballot && b1.ID > b2.ID {
		return true
	}
	return false
}

// Paper Figure 4:
//	Upon timeout of startTimer
//  Receiver implementation:
//  1. insert (b, qc) into ballots
//  2. if |ballots| ≥ majority then checkLeader()
// else qc ← false
//  3. clear ballots, r ← r + 1
//  4. send ⟨HBRequest, r⟩ to all peers,
//  5. startTimer(delay)

// Upon receiving ⟨HBRequest⟩ from server s
// rnd: round of this request
// Receiver implementation:
// Send ⟨HBReply, rnd, b, qc⟩ to s

// Upon receiving ⟨HBReply⟩ from server s
// rnd: round this reply was sent in
// ballot: ballot number of server s
// q: quorum-connected flag of server s
// Receiver implementation:
// 1. if rnd = r then insert (ballot, q) into ballots

// schedule a timeout event in d timeunits.
// When starting: send ⟨HBRequest, r⟩ to all
// peers and startTimer(delay)
func (op *OmniPaxos) startTimer() {
	for !op.killed() {
		// 5. startTimer(delay) - sleep for delay period FIRST have better success rate than sleep last
		time.Sleep(time.Duration(op.delay) * time.Millisecond)

		// Upon timeout of startTimer (Figure 4 algorithm)
		op.mu.Lock()
		// 1. insert (b, qc) into ballots
		op.ballots = append(op.ballots, BallotInfo{Ballot: op.b, QC: op.qc})

		// 2. if |ballots| ≥ majority then checkLeader() else qc ← false
		majority := len(op.peers)/2 + 1
		if len(op.ballots) >= majority {
			op.checkLeader()
		} else {
			op.qc = false
		}

		// 3. clear ballots, r ← r + 1
		op.ballots = make([]BallotInfo, 0)
		op.r++
		currentRound := op.r
		op.mu.Unlock()

		// 4. send ⟨HBRequest, r⟩ to all peers - simplified version
		for i := range op.peers {
			if i != op.me {
				args := &HBRequestArgs{
					Rnd: currentRound,
				}
				reply := &HBRequestReply{}

				// Send heartbeat synchronously to avoid race conditions
				ok := op.peers[i].Call("OmniPaxos.Heartbeat", args, reply)
				if ok {
					op.mu.Lock()
					// Upon receiving ⟨HBReply⟩: if rnd = r then insert (ballot, q) into ballots
					if reply.Rnd == currentRound && reply.Rnd == op.r {
						op.ballots = append(op.ballots, BallotInfo{Ballot: reply.Ballot, QC: reply.Q})
					}
					op.mu.Unlock()
				}
			}
		}
	}
}

// checkLeader() Implementation (Figure 4):
//
//	let candidates ← ballots with qc = true
//	let max ←max(candidates)
//
// if max < l then increment(b) s.t. b > l,
// set qc ← true
// else if max > l then l ← max,
// trigger ⟨Leader, max.pid, max⟩
func (op *OmniPaxos) checkLeader() {
	// let candidates ← ballots with qc = true
	var candidates []BallotNumber

	// Include my own ballot if I have qc = true again ????
	if op.qc {
		candidates = append(candidates, op.b)
	}
	// // useless code since ballot from self is always added right before checkLeader() is called in startTimer()
	// maybenot, it will make sure ballot not get lost???? TODO ASK TA

	// Include other servers' ballots if they have qc = true
	for _, ballotInfo := range op.ballots {
		if ballotInfo.QC {
			candidates = append(candidates, ballotInfo.Ballot)
		}
	}

	// log.Info().Msgf("[%d] checkLeader: found %d candidates with qc=true (my ballot=%v, qc=%v), current leader=%v",
	// 	op.me, len(candidates), op.b, op.qc, op.l)

	if len(candidates) == 0 {
		// No candidates with qc = true, so I should set my qc = true to become a candidate
		log.Info().Msgf("[%d] No candidates found, setting qc=true for myself", op.me)
		op.qc = true
		return
	}

	// let max ← max(candidates)
	max := op.max(candidates)
	// log.Info().Msgf("[%d] checkLeader: max candidate=%v, current leader=%v", op.me, max, op.l)

	// BLE Algorithm logic:
	// if max < l then increment(b) s.t. b > l, set qc ← true
	// else if max > l then l ← max, trigger ⟨Leader, max.pid, max⟩
	// else (max == l) no action needed - current leader is still valid

	if op.l.Greater(max) {
		// if max < l then increment(b) s.t. b > l, set qc ← true
		log.Info().Msgf("[%d] Max %v < current leader l = %v, incrementing ballot from b = %v", op.me, max, op.l, op.b)
		op.incrementBallot()
		op.qc = true
		log.Info().Msgf("[%d] Incremented ballot to b = %v, qc=true", op.me, op.b)
	} else if max.Greater(op.l) {
		// else if max > l then l ← max, trigger ⟨Leader, max.pid, max⟩
		op.l = max
		log.Info().Msgf("[%d] Electing new leader: %v", op.me, max)
		// trigger ⟨Leader, max.pid, max⟩ - send to the elected server
		//if max.ID < len(op.peers) { in case of index out of range
		// Release mutex before making RPC call to avoid deadlock
		leaderRequest := &LeaderRequest{S: max.ID, N: max}
		op.mu.Unlock()
		op.peers[max.ID].Call("OmniPaxos.Leader", leaderRequest, &LeaderReply{})
		op.mu.Lock()
		//}
	}
	// else max == l: no action needed - current leader is still valid
}

// increment the sequence number of ballot b to be greater than the current leader ballot l
func (op *OmniPaxos) incrementBallot() {
	if op.l.Ballot >= op.b.Ballot {
		op.b.Ballot = op.l.Ballot + 1
	} else {
		op.b.Ballot++
	}
	op.b.ID = op.me // i want to be the leader
}

// max(ballots) return maximum ballot based on lexicographic order
func (op *OmniPaxos) max(ballots []BallotNumber) BallotNumber {
	var max BallotNumber
	for _, ballot := range ballots {
		if ballot.Greater(max) {
			max = ballot
		}
	}
	return max
}

// ⟨Leader⟩ from BLE
// Fields:
//
//	s the elected server
//	n the round s got elected in
//	Leader implementation (if s = self AND n > promisedRnd):
//	1. reset all volatile state of leader
//	2. state ← (LEADER, PREPARE), currentRnd ← n, promisedRnd ← n

func (op *OmniPaxos) Leader(args *LeaderRequest, reply *LeaderReply) {
	op.mu.Lock()
	defer op.mu.Unlock()

	log.Info().Msgf("[%d] Leader RPC called: S=%d, N=%v, my promisedRnd=%v", op.me, args.S, args.N, op.promisedRnd)

	// Only process if I am the elected leader and ballot is greater than promised
	if args.S == op.me && args.N.Greater(op.promisedRnd) {
		log.Info().Msgf("[%d] Leader RPC received: I AM THE LEADER! Ballot=%v", op.me, args.N)
		// 1. Reset all volatile state of leader,

		// 2. state ← (LEADER, PREPARE), currentRnd ← n, promisedRnd ← n
		op.l = args.N
		op.currentRound = args.N
		op.promisedRnd = args.N

	} else {
		log.Info().Msgf("[%d] Leader RPC ignored: not me or N <= promisedRnd, Ballot=%v",
			op.me, args.N)
	}
}
