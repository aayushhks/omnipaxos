package omnipaxos

//
// Support for Omnipaxos to save persistent state (log &c) and k/v server snapshots.
//
// We will use the original persister.go to test your code for grading.
// So, while you can modify this code to help you debug, please
// test with the original before submitting.
//

import "sync"

type Persister struct {
	mu       sync.Mutex
	state    []byte
	snapshot []byte
}

func MakePersister() *Persister {
	return &Persister{}
}

func clone(orig []byte) []byte {
	x := make([]byte, len(orig))
	copy(x, orig)
	return x
}

func (ps *Persister) Copy() *Persister {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	np := MakePersister()
	np.state = ps.state
	np.snapshot = ps.snapshot
	return np
}

func (ps *Persister) SaveState(state []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.state = clone(state)
}

func (ps *Persister) ReadState() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return clone(ps.state)
}

func (ps *Persister) StateSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.state)
}

// Save both state and K/V snapshot as a single atomic action,
// to help avoid them getting out of sync.
func (ps *Persister) SaveStateAndSnapshot(state []byte, snapshot []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.state = clone(state)
	ps.snapshot = clone(snapshot)
}

func (ps *Persister) ReadSnapshot() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return clone(ps.snapshot)
}

func (ps *Persister) SnapshotSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.snapshot)
}
