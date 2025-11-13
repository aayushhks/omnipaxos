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
	// New A4 fields
	currentRnd Ballot
	promises   map[int]*Promise
	maxProm    *Promise
	accepted   []int
	buffer     []interface{}

	os OmniPaxosState

	r       int
	b       Ballot
	qc      bool
	delay   time.Duration
	ballots map[Ballot]bool

	state State

	applyCh chan ApplyMsg

	linkDrop       bool
	missedHbCounts map[int]int

	restart Restart
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

// --- A4 RPC Structs ---
type PrepareRequest struct {
	L      int
	N      Ballot
	AccRnd Ballot
	LogIdx int
	DecIdx int
}

type DummyReply struct{}

type PromiseRequest struct {
	F      int
	N      Ballot
	AccRnd Ballot
	LogIdx int
	DecIdx int
	Sfx    []interface{}
}

type AcceptSyncRequest struct {
	L       int
	N       Ballot
	Sfx     []interface{}
	SyncIdx int
}

type DecideRequest struct {
	L      int
	N      Ballot
	DecIdx int
}

type AcceptedRequest struct {
	F      int
	N      Ballot
	LogIdx int
}

type AcceptRequest struct {
	L   int
	N   Ballot
	Idx int
	C   interface{}
}

type PrepareReqRequest struct {
	F int
}

type ReconnectedRequest struct {
	F int
}

// --- End A4 Structs ---

type OmniPaxosState struct {
	L           Ballot
	Log         []interface{} // Expanded for A4
	PromisedRnd Ballot
	AcceptedRnd Ballot // Expanded for A4
	DecidedIdx  int    // Expanded for A4
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
	op.Debug("returning GetState, ballot:%d, isLeader:%t, state:%+v, rs:%+v", ballot, isleader, op.state, op.os)
	return ballot, isleader
}

func (op *OmniPaxos) Proposal(command interface{}) (int, int, bool) {
	// To be implemented
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

func (op *OmniPaxos) checkLeader() {
	candidates := map[Ballot]bool{}
	for b, q := range op.ballots {
		if q {
			candidates[b] = true
		}
	}

	max := op.max(candidates)
	L := op.os.L
	op.Trace("inside checkLeader, round:%d, I:n:%d, I:pid:%d, max:n:%d, max:pid:%d, compare:%d, ballots:%+v", op.r, L.N, L.Pid, max.N, max.Pid, max.compare(L), op.ballots)

	if max.compare(L) < 0 {
		op.increment(&op.b, L.N)
		op.qc = true
		op.Trace("setting qc round:%d, max:N:%d, max:pid:%d - previous leader:N:%d, pid:%d, promisedRnd:%d, ballots:%+v, state:%+v", op.r, max.N, max.Pid, L.N, L.Pid, op.os.PromisedRnd, op.ballots, op.state)
	} else if max.compare(L) > 0 {
		op.Info("setting leader for round:%d, max:N:%d, max:pid:%d - previous leader:N:%d, pid:%d, promisedRnd:%d, ballots:%+v, state:%+v", op.r, max.N, max.Pid, L.N, L.Pid, op.os.PromisedRnd, op.ballots, op.state)
		op.os.L = max
		op.persist()
		op.Debug("updated leader L:%+v", op.os.L)
		op.triggerLeader(max.Pid, max)
	}
}

func (op *OmniPaxos) triggerLeader(s int, n Ballot) {
	if !(s == op.me && n.compare(op.os.PromisedRnd) > 0) {
		op.state.role = FOLLOWER
		return
	}
	op.Info("making itself as leader")

	// 1. reset state
	op.accepted = make([]int, len(op.accepted))
	op.promises = map[int]*Promise{}
	op.maxProm = &Promise{}
	op.buffer = []interface{}{}

	// 2.
	op.qc = true
	op.state = State{role: LEADER, phase: PREPARE}
	op.currentRnd = n
	op.os.PromisedRnd = n

	// 3. add own promise
	promise := Promise{f: op.me, accRnd: op.os.AcceptedRnd, logIdx: len(op.os.Log),
		decIdx: op.os.DecidedIdx, log: op.suffix(op.os.DecidedIdx)}
	op.promises[op.me] = &promise

	// 4. send prepare to all peers
	for peer := range op.peers {
		if peer != op.me {
			go func(peer int, pid int, currentRnd Ballot, acceptedRnd Ballot, logSize int, decidedIdx int) {
				request := PrepareRequest{
					L:      pid,
					N:      currentRnd,
					AccRnd: acceptedRnd,
					LogIdx: logSize,
					DecIdx: decidedIdx,
				}
				op.peers[peer].Call("OmniPaxos.PrepareHandler", &request, &DummyReply{})
			}(peer, op.me, op.currentRnd, op.os.AcceptedRnd, len(op.os.Log), op.os.DecidedIdx)
		}
	}

	op.persist()
}

func (op *OmniPaxos) PrepareHandler(req *PrepareRequest, reply *DummyReply) {
	go func(l int, n Ballot, accRnd Ballot, logIdx int, decIdx int) {
		op.mu.Lock()
		defer func() { op.mu.Unlock() }()

		op.Debug("Handle prepare, l:%d, n:%+v, accRnd:%d, logIdx:%d, decIdx:%d, promRnd:%+v",
			l, n, accRnd, logIdx, decIdx, op.os.PromisedRnd)

		// 1.
		if op.os.PromisedRnd.compare(n) > 0 {
			return
		}

		// 2. update state
		op.state = State{
			role:  FOLLOWER,
			phase: PREPARE,
		}

		// 3. updated promised round
		op.os.PromisedRnd = n
		op.persist()

		// 4. find suffix
		var sfx []interface{}
		if op.os.AcceptedRnd.compare(accRnd) > 0 {
			sfx = op.suffix(decIdx)
		} else if op.os.AcceptedRnd.compare(accRnd) == 0 {
			sfx = op.suffix(logIdx)
		} else {
			sfx = []interface{}{}
		}

		// 5. send promise to leader
		request := PromiseRequest{
			F:      op.me,
			N:      n,
			AccRnd: op.os.AcceptedRnd,
			LogIdx: len(op.os.Log),
			DecIdx: op.os.DecidedIdx,
			Sfx:    sfx,
		}
		op.peers[l].Call("OmniPaxos.PromiseHandler", &request, &DummyReply{})
	}(req.L, req.N, req.AccRnd, req.LogIdx, req.DecIdx)
}

func (op *OmniPaxos) PromiseHandler(req *PromiseRequest, reply *DummyReply) {
	go func(f int, n Ballot, accRnd Ballot, logIdx int, decIdx int, sfx []interface{}) {
		op.mu.Lock()
		defer func() { op.mu.Unlock() }()

		op.Debug("Handle Promise, f:%d, n:%+v, accRnd:%d, logIdx:%d, decIdx:%d, sfx:%+v",
			f, n, accRnd, logIdx, decIdx, sfx)

		// 1.
		if n.compare(op.currentRnd) != 0 {
			return
		}

		// 2. update promise
		promise := Promise{
			f:      f,
			accRnd: accRnd,
			logIdx: logIdx,
			decIdx: decIdx,
			log:    sfx,
		}
		op.promises[f] = &promise

		// return if not a leader
		if op.state.role != LEADER {
			return
		}

		if op.state.phase == PREPARE {

			// P1. return if there are no majority yet
			if len(op.promises) < (len(op.peers)+1)/2 {
				return
			}

			// P2. find the maximum promise (with accRnd and logIdx)
			op.maxProm = op.maxPromise()

			// P3. remove extra logs if the leaders accepted round is not same as maximum promises
			if op.maxProm.accRnd != op.os.AcceptedRnd {
				op.os.Log = op.prefix(op.os.DecidedIdx)
			}

			// P4. append max promise's suffix to the log
			op.os.Log = append(op.os.Log, op.maxProm.log...)

			// P5. append buffer to the log unless it is stopped
			if op.stopped() {
				op.buffer = []interface{}{}
			} else {
				op.os.Log = append(op.os.Log, op.buffer...)
			}

			// P6. set accpeted round to current round, updated accepted for leader and set state
			op.os.AcceptedRnd = op.currentRnd
			op.accepted[op.me] = len(op.os.Log)
			op.state = State{role: LEADER, phase: ACCEPT}

			// P5. send AcceptSync to each promised follower
			for _, p := range op.promises {
				var syncIdx int
				if p.accRnd == op.maxProm.accRnd {
					syncIdx = p.logIdx
				} else {
					syncIdx = p.decIdx
				}

				go func(l int, n Ballot, sfx []interface{}, syncIdx int, f int) {
					request := AcceptSyncRequest{
						L:       l,
						N:       n,
						Sfx:     sfx,
						SyncIdx: syncIdx,
					}
					// op.peers[f].Call("OmniPaxos.AcceptSyncHandler", &request, &DummyReply{}) // To be added
				}(op.me, n, op.suffix(syncIdx), syncIdx, p.f)
			}

		} else if op.state.phase == ACCEPT {
			// Logic to be added
		}

		op.persist()

	}(req.F, req.N, req.AccRnd, req.LogIdx, req.DecIdx, req.Sfx)
}

func (op *OmniPaxos) maxPromise() *Promise {
	max := &Promise{accRnd: Ballot{N: -1, Pid: -1}, log: []interface{}{}}
	for _, p := range op.promises {
		if (p.accRnd.compare(max.accRnd) > 0) || (p.accRnd == max.accRnd && p.logIdx > max.logIdx) {
			max = p
		}
	}
	return max
}

// --- A4 Log Helpers ---
func (op *OmniPaxos) suffix(idx int) []interface{} {
	if idx > len(op.os.Log) {
		return []interface{}{}
	}
	return op.os.Log[idx:]
}

func (op *OmniPaxos) prefix(idx int) []interface{} {
	if idx > len(op.os.Log) {
		idx = len(op.os.Log)
	}
	return op.os.Log[:idx]
}

// --- End Log Helpers ---

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
			op.checkLeader()
		} else {
			op.qc = false
		}

		op.updateMissedHbsAndReconnect() // Stub for A4

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

func (op *OmniPaxos) checkRecover() {
	// Stub for A4
}

func (op *OmniPaxos) stopped() bool {
	// Stub for A4
	return false
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

	// A3 fields
	op.delay = time.Millisecond * 100
	op.qc = true
	op.r = 0
	op.b = Ballot{N: 0, Pid: me}
	op.ballots = make(map[Ballot]bool)

	// A4 fields
	op.accepted = make([]int, len(peers))
	op.promises = map[int]*Promise{}
	op.buffer = []interface{}{}

	op.state = State{role: FOLLOWER, phase: PREPARE}
	// Initialize OmniPaxosState with A4 fields
	op.os = OmniPaxosState{L: Ballot{N: -1, Pid: -1}, Log: []interface{}{}, PromisedRnd: Ballot{N: -1, Pid: -1}, AcceptedRnd: Ballot{N: -1, Pid: -1}, DecidedIdx: 0}

	op.missedHbCounts = make(map[int]int)
	op.restart = Restart{}

	op.readPersist()
	// op.addToApplyChan(op.os.Log, 0, op.os.DecidedIdx) // A4

	go op.startTimer(op.delay)

	// go func() { // A4
	// 	time.Sleep(op.delay)
	// 	op.checkRecover()
	// }()

	return op
}
