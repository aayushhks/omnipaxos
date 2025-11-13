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

	op.readPersist()

	// go op.startTimer(op.delay) // Will be added in next commit

	return op
}
