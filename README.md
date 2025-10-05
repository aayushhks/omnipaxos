# OmniPaxos

## Introduction

In this series of assignments you'll implement OmniPaxos, a replicated state machine protocol. A replicated service achieves fault tolerance by storing complete copies of its state (i.e., data) on multiple replica servers. Replication allows the service to continue operating even if some of its servers experience failures (crashes or a broken or flaky network). The challenge is that failures may cause the replicas to hold differing copies of the data.

OmniPaxos organizes client requests into a sequence, called the log, and ensures that all the replica servers see the same log. Each replica executes client requests in log order, applying them to its local copy of the service's state. Since all the live replicas see the same log contents, they all execute the same requests in the same order, and thus continue to have identical service state. If a server fails but later recovers, OmniPaxos takes care of bringing its log up to date. OmniPaxos will continue to operate as long as at least a majority of the servers are alive and can talk to each other. If there is no such majority, OmniPaxos will make no progress, but will pick up where it left off as soon as a majority can communicate again.

In these assignments you'll implement OmniPaxos as a Go object type with associated methods, meant to be used as a module in a larger service. A set of OmniPaxos instances talk to each other with RPC to maintain replicated logs. Your OmniPaxos interface will support an indefinite sequence of numbered commands, also called log entries. The entries are numbered with index numbers. The log entry with a given index will eventually be committed. At that point, your OmniPaxos should send the log entry to the larger service for it to execute.

You should follow the design in the [OmniPaxos paper](https://dl.acm.org/doi/pdf/10.1145/3552326.3587441), with particular attention to Figure 3 and Figure 4. You'll implement most of what's in the paper.

This assignment is due in two parts. You must submit each part on the corresponding due date. Please create one repository, `a3-YourName`, for both assignments. A4 builds directly on A3.

## Getting Started

We supply you with skeleton code in `omnipaxos.go`. We also supply a set of tests, which you should use to drive your implementation efforts, and which we'll use to grade your submitted assignment.

To get up and running, execute the following commands. Don't forget the `git pull` to get the latest software.

```
$ go test -race
Test (3): initial election ...
--- FAIL: TestInitialElection3 (5.04s)
        config.go:326: expected one leader, got none
Test (3): election after network failure ...
--- FAIL: TestReElection3 (5.03s)
        config.go:326: expected one leader, got none
...
```

To run a specific set of tests, use `go test -race -run 3` or `go test -race -run TestInitialElection3`.

## The Code

Implement OmniPaxos by adding code to `omnipaxos.go`. In that file you'll find skeleton code, plus examples of how to send and receive RPCs.

Your implementation must support the following interface, which the Tester will use. You'll find more details in comments in `ominpaxos.go`.

```
// create a new OmniPaxos server instance:
op := Make(peers, me, persister, applyCh)

// start agreement on a new log entry:
// See Figure 3.
op.Proposal(command interface{}) (index, ballot, isleader)

// ask a OmniPaxos for its current ballot, and whether it thinks it is leader
op.GetState() (ballot, isLeader)

// each time a new entry is committed to the log, each OmniPaxos peer
// should send an ApplyMsg to the service (or Tester).
type ApplyMsg
```

A service calls `Make(peers,me,…)` to create a OmniPaxos peer. The peers argument is an array of network identifiers of the OmniPaxos peers (including this one), for use with RPC. The `me` argument is the index of this peer in the peers array. `Start(command)` asks OmniPaxos to start the processing to append the command to the replicated log. `Start()` should return immediately, without waiting for the log appends to complete. The service expects your implementation to send an `ApplyMsg` for each newly committed log entry to the `applyCh` channel argument to `Make()`.

`omnipaxos.go` contains example code that sends an RPC (`sendHeartBeats()`) and that handles an incoming RPC (`HBRequest()`). Your OmniPaxos peers should exchange RPCs using the labrpc Go package (source in `labrpc`). The Tester can tell `labrpc` to delay RPCs, re-order them, and discard them to simulate various network failures. While you can temporarily modify `labrpc`, make sure your OmniPaxos works with the original `labrpc`, since that's what we'll use to test and grade your assignment. Your OmniPaxos instances must interact only with RPC; for example, they are not allowed to communicate using shared Go variables or files.

## A3: Ballot Leader Election

### Task

Implement OmniPaxos ballot leader election and heartbeats. The goal for A3 is for a single leader to be elected, for the leader to remain the leader if there are no failures, and for a new leader to take over if the old leader fails or if packets to/from the old leader are lost. Run `go test -run 3 -race` to test your A3 code.

### Hints

- You can't easily run your OmniPaxos implementation directly; instead you should run it by way of the Tester, i.e. `go test -run 3 -race`.
- Follow the paper's Figure 4 and Figure 3's `2. <Leader> from BLE` section. 
- Add the Figure 3 and 4 state for leader election to the `OmniPaxos` struct in `omnipaxos.go`. 
  - In order to make things convenient, try to use the same variable names and function names as mentioned in the Figure.
  - You'll also need to define a struct to hold information about each log entry.
- Fill in the `HBRequest` and `HBReply` structs. Modify `Make()` to create a background goroutine that will kick off leader election periodically by sending out `HeartBeat` RPCs when it hasn't heard from another peer for a while.
- Since each server needs to gossip with everyone else, try sending out `HBRequest`s to all servers (other than the sender itself) as individual Go routines, and then call `startTimer()`.
- An optimal value for the `delay` for `startTimer()` would be `100ms`.
- The Tester requires that the leader send heartbeat RPCs no more than ten times per second.
- You'll need to write code that takes actions periodically or after delays in time. The easiest way to do this is to create a goroutine with a loop that calls [time.Sleep()](https://golang.org/pkg/time/#Sleep); (see the `ticker()` goroutine that `Make()` creates for this purpose). Don't use Go's `time.Timer` or `time.Ticker`, which are difficult to use correctly.
- If your code has trouble passing the tests, read the paper's Figure 3 and 4 again; the full logic for ballot leader election is spread over multiple parts of the figure. But your primary focus is Figure 4 and Figure 3's `2. <Leader> from BLE` section.
- Don't forget to implement `GetState()`.
- The Tester calls your OmniPaxos's `op.Kill()` when it is permanently shutting down an instance. You can check whether `Kill()` has been called using `op.killed()`. You may want to do this in all loops, to avoid having dead OmniPaxos instances print confusing messages.
- Go RPC sends only struct fields whose names start with capital letters. Sub-structures must also have capitalized field names (e.g. fields of log records in an array).
- You might have RPC calls (such as `Leader`), which do not really need a reply. In such cases, you may create an empty struct (say `DummyReply`) to use as a placeholder.

### The Logger
- The repository provides you with a sophisticated logging mechanism via [ZeroLog](https://github.com/rs/zerolog). You can insert logs into your code using:
```go
log.Info().Msgf("Hello from OmniPaxos!")
```

You may also use the wrappers provided in `util.go`, which print the server IDs along with all logs so that you don't have to do so manually!

```go
op.Debug("Debug Log %v", args)
op.Info("Info Log %v", args)
op.Warn("Warn Log %v", args)
op.Fatal("Fatal Log %v", args)
```

Result:
```shell
12:14AM DBG [SERVER=2] Debug Log
12:14AM INF [SERVER=2] Info Log
12:14AM WRN [SERVER=2] Warn Log
12:14AM FTL [SERVER=2] Fatal Log
```

`zerolog` allows for logging at the following levels (from highest to lowest):
```
panic (zerolog.PanicLevel, 5)
fatal (zerolog.FatalLevel, 4)
error (zerolog.ErrorLevel, 3)
warn (zerolog.WarnLevel, 2)
info (zerolog.InfoLevel, 1)
debug (zerolog.DebugLevel, 0)
trace (zerolog.TraceLevel, -1)
```

When running tests, you can set the minimum allowed log level using the `loglevel` flag.

For example, the below command will only allow logs which are `Warn` or above.
```
go test -race -loglevel 2
```

Be sure you pass the A3 tests before submitting Part A3, so that you see something like this:

It is okay if you see some Warning logs from the tester such as disconnecting or connecting nodes. They are purely for debugging help.

```
$ go test -race -run 3
Test (3): [TestInitialElection3] initial election ...
  ... Passed --   4.6  3  273   32250    0
Test (3): [TestReElection3] election after network failure ...
  ... Passed --   7.7  3  515   46686    0
Test (3): [TestManyElections3] multiple elections ...
  ... Passed --  12.2  7 5563  472662    0
Test (3): [TestFigure5aQuorumLoss3] Partial Connectivity - Quorum-Loss Scenario (Fig 5a) ...
  ... Passed --   4.6  5  930   85040    0
Test (3): [TestFigure5bConstrainedElection3] Partial Connectivity - Constrained Election Scenario (Fig 5b) ...
  ... Passed --   4.6  5  916   79479    0
Test (3): [TestFigure5cChained3] Partial Connectivity - Chained Scenario (Fig 5c) ...
  ... Passed --   4.6  3  281   29023    0
PASS
ok  	a3-omnipaxos	39.583s
```

Each "Passed" line contains five numbers; these are the time that the test took in seconds, the number of OmniPaxos peers (usually 3 or 5), the number of RPCs sent during the test, the total number of bytes in the RPC messages, and the number of log entries that OmniPaxos reports were committed. Your numbers will differ from those shown here. You can ignore the numbers if you like, but they may help you sanity-check the number of RPCs that your implementation sends. For all of the parts the grading script will fail your solution if it takes more than 600 seconds for all of the tests (`go test`), or if any individual test takes more than 120 seconds.

## A4: Log Replication

### Task

Implement the log entries, so that the `go test -run 4 -race` tests pass.
For this, you need to implement everything remaining in Figure 3.

### Recovery (Important)
If a server has been disconnected from the entire network, it may miss out on some log entries that were committed in the meanwhile. To recover from this, Figure 3 mentions three steps. 
```
10. Upon Recovery
11. <PrepareReq> from Follower
12. <Reconnected> to server s
```

During the failure, another leader might have been elected and already completed the Prepare phase. Since the recovering server is unaware of who is the current leader, it sends `<PrepareReq>` to all its peers `10` . A receiving server that is the leader responds with `<Prepare>` `11` . From this point, the protocol proceeds as usual. Link session drops between two servers are handled similarly. Since they might be unaware that the other has become the leader during the disconnected period, a `<PrepareReq>` is sent `12`

This assignment only takes into account the situations where there are link session drops (aka. a complete network disconnection of a given node.)

The paper does not explicitly define when and how this behavior is triggered. You may choose to implement it however you want, but one possible approach would be the following:
1. Create a new variable in the OmniPaxos struct called `LinkDrop` (initialized to `false`).
2. During the heart beat exchange, if a node does not receive any heart beat for atleast `3` consecutive rounds, set `LinkDrop` to `true`.
3. During the heart beat exchange, if a node receives heart beats and `LinkDrop == true`:
   1. This implies that the given node has just recovered from a network disconnection.
   2. Set `LinkDrop` to `false`.
   3. Change the node's state to `FOLLOWER, RECOVER`.
   4. send `⟨PrepareReq⟩` to all peers
   5. Implement ` 11. <PrepareReq> from follower` (as described in Figure 3).

Using this design, you don't need a separate `12. <Reconnected>` implementation.


### This part contains two types of tests:

#### Don't Require A Recovery Mechanism
1. TestBasicAgree4 
2. TestTooManyCommandsAgree4 
3. TestRPCBytes4 
4. TestConcurrentStarts4 
5. TestCount4 
#### Require a Recovery Mechanism
6. TestFailAgree4
7. TestFailNoAgree4
8. TestBackup4
9. TestRejoin4
10. TestFig5aLogReplication4
11. TestFig5bLogReplication4
12. TestFig5cLogReplication4

Make sure you implement the Recovery Mechanism in order to pass all the tests.


### Hints
- All log entires are 0-indexed.
- This part involves multiple RPCs which once again, don't need replies. You may use the same `DummyReply` struct for all such RPC replies.
- Your first goal should be to pass `TestBasicAgree4()`. Start by implementing `Proposal()`, then write the code to send and receive new log entries via `<Proposal> -> <Accept> -> <Accepted> -> <Decide>` RPCs, following Figure 3. 
  - **But you will still need** a basic implementation of `<Prepare> -> <Promise> -> <AcceptSync>` to pass the first few tests, and a complete implementation to pass everything.
- Your code may have loops that repeatedly check for certain events. Don't have these loops execute continuously without pausing, since that will slow your implementation enough that it fails tests. Use Go's [condition variables](https://golang.org/pkg/sync/#Cond), or insert a `time.Sleep(10 * time.Millisecond)` in each loop iteration.
- Do yourself a favor and write (or re-write) code that's clean and clear. For ideas, re-visit the [Guidance page](https://cs-people.bu.edu/liagos/651-2022/labs/guidance.html) with tips on how to develop and debug your code.
- If you fail a test, look over the code for the test in `config.go` and `test_test.go` to get a better understanding what the test is testing. `config.go` also illustrates how the Tester uses the OmniPaxos API.
- Ensure than `prefix(idx)` and `suffix(idx)` are in line with your log indexing and do not throw out of bounds errors.
- Typically running the test several times will expose bugs related to non determinism.
The tests may fail your code if it runs too slowly. You can check how much real time and CPU time your solution uses with the `time` command. Here's typical output:
```
$ time go test -race -run 4 --loglevel 5
Test (4): [TestBasicAgree4] basic agreement ...
  ... Passed --   0.6  3   57    6086    2
Test (4): [TestTooManyCommandsAgree4] high traffic agreement (too many commands submitted fast) ...
  ... Passed --   8.2  3 2883  291988  299
Test (4): [TestRPCBytes4] RPC byte count ...
  ... Passed --   0.8  3  133  113846   10
Test (4): [TestFailAgree4] agreement despite follower disconnection ...
  ... Passed --   4.8  3  338   35313    6
Test (4): [TestFailNoAgree4] no agreement if too many followers disconnect ...
  ... Passed --   5.8  5 1319  140106    3
Test (4): [TestConcurrentStarts4] concurrent Proposal()s ...
  ... Passed --   1.6  3  147   16232    5
Test (4): [TestBackup4] leader backs up quickly over incorrect follower logs ...
  ... Passed --  55.0  5 12352  942357  122
Test (4): [TestCount4] RPC counts aren't too high ...
  ... Passed --   3.6  3  309   34458   11
Test (4): [TestRejoin4] rejoin of partitioned leader ...
  ... Passed --   6.0  3  431   43644    3
Test (4): [TestFig5aLogReplication4] Partial Connectivity - Quorum-Loss Scenario (Fig 5a) ...
  ... Passed --   5.7  5 1330  128700    5
Test (4): [TestFig5bLogReplication4] Partial Connectivity - Constrained Election Scenario (Fig 5b) ...
  ... Passed --   5.7  5 1287  120280    5
Test (4): [TestFig5cLogReplication4] Partial Connectivity - Chained Scenario (Fig 5c) ...
  ... Passed --   5.7  3  394   41744    5
PASS
ok  	a5-omnipaxos	94.833s
go test -run 4  10.32s user 5.81s system 8% cpu 3:11.40 total
```

The "ok OmniPaxos 94.833s" means that Go measured the time taken for the A4 tests to be 94.833 seconds of real (wall-clock) time. The "10.32s user" means that the code consumed 10.32s seconds of CPU time, or time spent actually executing instructions (rather than waiting or sleeping). If your solution uses an unreasonable amount of time, look for time spent sleeping or waiting for RPC timeouts, loops that run without sleeping or waiting for conditions or channel messages, or large numbers of RPCs sent.

#### A few other hints:

- Run git pull to get the latest lab software.
- Failures may be caused by problems in your code for A3 or log replication. Your code should pass all the A3 and A4 tests. It might be possible that your changes for 4A break some tests for A3. So keep testing both parts as you work on this assignment.

It is a good idea to run the tests multiple times before submitting and check that each run prints `PASS`.

```
$ go test -race -count 10
```
