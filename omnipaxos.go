package omnipaxos

import (
	"bytes"
	"encoding/gob"
	"sync"
	"sync/atomic"
	"time"

	"omnipaxos/labrpc"
)

// A Go object implementing a single OmniPaxos peer.
type OmniPaxos struct {
	mu            sync.Mutex
	peers         []*labrpc.ClientEnd
	persister     *Persister
	me            int
	dead          int32
	enableLogging int32

	os OmniPaxosState

	r       int
	b       Ballot
	qc      bool
	delay   time.Duration
	ballots map[Ballot]bool

	state State

	applyCh chan ApplyMsg

	// Fields added for A4 later
	missedHbCounts map[int]int
	restart        Restart
}

type Restart struct {
	loop int
}

type Promise struct {
	f      int
	accRnd Ballot
	logIdx int
	decIdx int
	log    []interface{}
}

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type HBRequest struct {
	Rnd int
}

type HBReply struct {
	Rnd    int
	Ballot int
	Q      bool
}

type DummyReply struct{}

type OmniPaxosState struct {
	L           Ballot
	Log         []interface{}
	PromisedRnd Ballot
	AcceptedRnd Ballot
	DecidedIdx  int
}

func (r *OmniPaxosState) toBytes() ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(r)
	return buf.Bytes(), err
}

func omnipaxosStatefromBytes(b []byte) (OmniPaxosState, error) {
	buf := bytes.NewBuffer(b)
	dec := gob.NewDecoder(buf)
	s := OmniPaxosState{}
	err := dec.Decode(&s)
	return s, err
}

type Ballot struct {
	N   int
	Pid int
}

func (b *Ballot) compare(o Ballot) int {
	if b.N < o.N {
		return -1
	} else if b.N == o.N {
		if b.Pid < o.Pid {
			return -1
		} else if b.Pid == o.Pid {
			return 0
		}
		return 1
	}
	return 1
}

const (
	// role
	LEADER   = "LEADER"
	FOLLOWER = "FOLLOWER"

	// phase
	PREPARE = "PREPARE"
	ACCEPT  = "ACCEPT"
	RECOVER = "RECOVER"
)

type State struct {
	role  string
	phase string
}

func (op *OmniPaxos) HeartBeatHandler(args *HBRequest, reply *HBReply) {
	op.mu.Lock()
	defer func() { op.mu.Unlock() }()

	reply.Ballot = op.b.N
	reply.Q = op.qc
	reply.Rnd = args.Rnd
}

func (op *OmniPaxos) GetState() (int, bool) {
	op.mu.Lock()
	defer func() { op.mu.Unlock() }()

	var ballot int
	var isleader bool

	ballot = op.b.N
	isleader = (op.state.role == LEADER) && (op.os.L.Pid == op.me)
	return ballot, isleader
}

func (op *OmniPaxos) Proposal(command interface{}) (int, int, bool) {
	return -1, -1, false
}

func (op *OmniPaxos) Kill() {
	atomic.StoreInt32(&op.dead, 1)
	atomic.StoreInt32(&op.enableLogging, 0)
}

func (op *OmniPaxos) killed() bool {
	z := atomic.LoadInt32(&op.dead)
	return z == 1
}

func (op *OmniPaxos) max(m map[Ballot]bool) Ballot {
	res := Ballot{N: -1, Pid: -2}
	for b := range m {
		if res.compare(b) < 0 {
			res = b
		}
	}
	return res
}

func (op *OmniPaxos) increment(ballot *Ballot, I int) {
	ballot.N = I + 1
}

func (op *OmniPaxos) startTimer(delay time.Duration) {
	for {
		op.mu.Lock()

		// 1. insert own ballot
		op.ballots[op.b] = op.qc

		// 2. if have majority, then check leader else qc is false
		if len(op.ballots) >= (len(op.peers)+1)/2 {
			// op.checkLeader() // Will be added in next commit
		} else {
			op.qc = false
		}

		// op.updateMissedHbsAndReconnect() // Will be added in A4

		// 3. clear ballot and increase the round
		op.ballots = make(map[Ballot]bool)
		op.r++
		op.mu.Unlock()

		// 4. send heartbeat to all peers
		for peer := range op.peers {
			if peer != op.me {
				go func(r int, p int) {
					request := HBRequest{Rnd: r}
					reply := HBReply{}
					ok := op.peers[p].Call("OmniPaxos.HeartBeatHandler", &request, &reply)
					op.Trace("received heart beat from %d, round:%d, ballot:%d, q:%t", p, reply.Rnd, reply.Ballot, reply.Q)

					if ok && reply.Rnd == r {
						op.mu.Lock()
						op.ballots[Ballot{N: reply.Ballot, Pid: p}] = reply.Q
						op.mu.Unlock()
					}
				}(op.r, peer)
			}
		}

		time.Sleep(delay)
	}

}

func (op *OmniPaxos) updateMissedHbsAndReconnect() {
	// Stub for A4
	op.restart.loop++
}

func (op *OmniPaxos) persist() {
	buf, _ := op.os.toBytes()
	op.persister.SaveState(buf)
}

func (op *OmniPaxos) readPersist() {
	op.os, _ = omnipaxosStatefromBytes(op.persister.ReadState())
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *OmniPaxos {

	op := &OmniPaxos{}
	op.peers = peers
	op.persister = persister
	op.me = me
	op.applyCh = applyCh

	op.enableLogging = 0
	op.Info("initializing Omni Paxos instance")

	op.delay = time.Millisecond * 100
	op.qc = true
	op.r = 0
	op.b = Ballot{N: 0, Pid: me}
	op.ballots = make(map[Ballot]bool)

	op.state = State{role: FOLLOWER, phase: PREPARE}
	op.os = OmniPaxosState{L: Ballot{N: -1, Pid: -1}, Log: []interface{}{}, PromisedRnd: Ballot{N: -1, Pid: -1}, AcceptedRnd: Ballot{N: -1, Pid: -1}, DecidedIdx: 0}

	// Init A4 fields
	op.missedHbCounts = make(map[int]int)
	op.restart = Restart{}

	op.readPersist()

	go op.startTimer(op.delay)

	return op
}
