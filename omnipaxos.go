package omnipaxos

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"omnipaxos/labrpc"
)

type ServerState int

const (
	Follower  ServerState = iota
	Candidate
	Leader
)

type HBRequest struct {
	Ballot int
	Leader int
}

type HBReply struct {
	Accepted bool
}

type OmniPaxos struct {
	mu            sync.Mutex
	peers         []*labrpc.ClientEnd
	persister     *Persister
	me            int
	dead          int32
	enableLogging int32

	state       ServerState
	ballot      int
	leader      int
	heartbeatCh chan bool
}

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

func (op *OmniPaxos) GetState() (int, bool) {
	op.mu.Lock()
	defer op.mu.Unlock()
	return op.ballot, op.state == Leader
}

func (op *OmniPaxos) Proposal(command interface{}) (int, int, bool) {
	index := -1
	ballot := -1
	isLeader := false
	return index, ballot, isLeader
}

func (op *OmniPaxos) Kill() {
	atomic.StoreInt32(&op.dead, 1)
	atomic.StoreInt32(&op.enableLogging, 0)
}

func (op *OmniPaxos) killed() bool {
	z := atomic.LoadInt32(&op.dead)
	return z == 1
}

func (op *OmniPaxos) HBRequest(args *HBRequest, reply *HBReply) {
	op.mu.Lock()
	defer op.mu.Unlock()

	accepted := false
	if args.Ballot > op.ballot {
		op.ballot = args.Ballot
		op.leader = args.Leader
		op.state = Follower
		accepted = true
	} else if args.Ballot == op.ballot {
		if op.leader == -1 || args.Leader >= op.leader {
			op.leader = args.Leader
			op.state = Follower
			accepted = true
		}
	}

	reply.Accepted = accepted

	if accepted {
		select {
		case op.heartbeatCh <- true:
		default:
		}
	}
}

// sendHeartbeatsAndCheckQuorum uses a time-bounded WaitGroup for a robust, non-blocking check.
func (op *OmniPaxos) sendHeartbeatsAndCheckQuorum(currentBallot int) bool {
	quorumSize := len(op.peers)/2 + 1
	var acks int32 = 1

	var wg sync.WaitGroup
	wg.Add(len(op.peers) - 1)

	for i := range op.peers {
		if i == op.me {
			continue
		}
		go func(server int) {
			defer wg.Done()
			args := HBRequest{Ballot: currentBallot, Leader: op.me}
			reply := HBReply{}
			ok := op.peers[server].Call("OmniPaxos.HBRequest", &args, &reply)
			if ok && reply.Accepted {
				atomic.AddInt32(&acks, 1)
			}
		}(i)
	}

	// This is a standard Go pattern for a timed WaitGroup.
	// It prevents the leader from blocking indefinitely on a partitioned peer.
	c := make(chan struct{})
	go func() {
		defer close(c)
		wg.Wait()
	}()

	select {
	case <-c:
		// All RPCs completed (or timed out by labrpc) within our window.
	case <-time.After(90 * time.Millisecond):
		// Our own shorter timeout fired. We must make a decision with the acks we have.
	}

	finalAcks := atomic.LoadInt32(&acks)
	return int(finalAcks) >= quorumSize
}

func (op *OmniPaxos) ticker() {
	const heartbeatInterval = 100 * time.Millisecond

	for !op.killed() {
		op.mu.Lock()
		state := op.state
		op.mu.Unlock()

		switch state {
		case Leader:
			op.mu.Lock()
			currentBallot := op.ballot
			op.mu.Unlock()

			hasQuorum := op.sendHeartbeatsAndCheckQuorum(currentBallot)

			op.mu.Lock()
			if op.state == Leader && op.ballot == currentBallot {
				if !hasQuorum {
					op.Info("S%d lost quorum for B%d, stepping down.", op.me, currentBallot)
					op.state = Follower
					op.leader = -1
				}
			}
			op.mu.Unlock()

			time.Sleep(heartbeatInterval)

		case Follower, Candidate:
			electionTimeout := time.Duration(400+(rand.Intn(200))) * time.Millisecond

			select {
			case <-op.heartbeatCh:
				op.mu.Lock()
				op.state = Follower
				op.mu.Unlock()
			case <-time.After(electionTimeout):
				op.mu.Lock()
				op.state = Candidate
				op.ballot++
				op.leader = op.me
				currentBallot := op.ballot
				op.Info("S%d starting election for ballot B%d", op.me, currentBallot)
				op.mu.Unlock()

				hasQuorum := op.sendHeartbeatsAndCheckQuorum(currentBallot)

				op.mu.Lock()
				if op.state == Candidate && op.ballot == currentBallot {
					if hasQuorum {
						op.Info("S%d won election for B%d", op.me, op.ballot)
						op.state = Leader
					} else {
						op.state = Follower
						op.leader = -1
					}
				}
				op.mu.Unlock()
			}
		}
	}
}

func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *OmniPaxos {
	op := &OmniPaxos{}
	op.peers = peers
	op.persister = persister
	op.me = me

	op.state = Follower
	op.ballot = 0
	op.leader = -1
	op.heartbeatCh = make(chan bool, 1)
	op.enableLogging = 1

	go op.ticker()

	return op
}