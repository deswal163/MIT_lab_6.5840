package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.

import (
	//	"bytes"
	"math/rand"
	"sync"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

const (
	follower = iota
	candidate
	leader
)

type LogEntry struct {
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
// CurrentTerm and VotedFor are exported for labgob persistence in 3C.
// All other fields are in-memory only.
type Raft struct {
	mu               sync.Mutex          // Lock to protect shared access to this peer's state
	peers            []*labrpc.ClientEnd // RPC end points of all peers
	persister        *tester.Persister   // Object to hold this peer's persisted state
	me               int                 // this peer's index into peers[]
	CurrentTerm      int
	VotedFor         int
	leaderId         int
	state            int
	lastHeartBeat    time.Time
	electionDeadline time.Time
	// logs
	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

func (rf *Raft) updateHeartBeat() {
	rf.lastHeartBeat = time.Now()
	rf.electionDeadline = time.Now().Add(generateRandomTimeout())
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool

	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.CurrentTerm
	isleader = (rf.state == leader)

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int

	// Your data here (3A, 3B).
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
	// Your data here (3A).
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.CurrentTerm < args.Term {
		rf.state = follower
		rf.CurrentTerm = args.Term
		rf.VotedFor = -1
	}

	reply.Term = rf.CurrentTerm

	if args.Term < rf.CurrentTerm || (rf.VotedFor != -1 && rf.VotedFor != args.CandidateId) {
		reply.VoteGranted = false
		return
	}

	rf.VotedFor = args.CandidateId
	rf.state = follower
	reply.VoteGranted = true
	rf.updateHeartBeat()
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.CurrentTerm
	reply.Success = false

	if args.Term < rf.CurrentTerm {
		return
	}

	rf.leaderId = args.LeaderId
	rf.state = follower
	rf.updateHeartBeat()

	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.VotedFor = -1
	}

	reply.Term = rf.CurrentTerm
	reply.Success = true
}

func (rf *Raft) sendHeartBeats() {

	for {
		rf.mu.Lock()
		if rf.state != leader {
			rf.mu.Unlock()
			return
		}

		for i := range rf.peers {
			if i == rf.me {
				continue
			}

			go func(i, currentTerm, leaderId int) {
				args := AppendEntriesArgs{
					Term:     currentTerm,
					LeaderId: leaderId,
					Entries:  nil,
				}

				reply := AppendEntriesReply{}

				ok := rf.sendAppendEntries(i, &args, &reply)

				if !ok {
					return
				}

				rf.mu.Lock()
				defer rf.mu.Unlock()

				if reply.Term > rf.CurrentTerm {
					rf.CurrentTerm = reply.Term
					rf.state = follower
					rf.VotedFor = -1
					rf.updateHeartBeat()
				}

			}(i, rf.CurrentTerm, rf.me)
		}

		rf.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
}

func generateRandomTimeout() time.Duration {
	return time.Duration((300 + rand.Intn(200)) * int(time.Millisecond))
}

func (rf *Raft) startElection(currentTerm, me int) {

	// votesReceived is accessed only under rf.mu; do not replace with atomic.
	votesReceived := 1
	savedCurrentTerm := currentTerm

	for i := range rf.peers {
		if i == me {
			continue
		}

		go func(i, currentTerm, candidateId int) {
			args := RequestVoteArgs{
				Term:        currentTerm,
				CandidateId: candidateId,
			}

			reply := RequestVoteReply{}

			ok := rf.sendRequestVote(i, &args, &reply)
			if !ok {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if rf.state != candidate || rf.CurrentTerm != savedCurrentTerm {
				return
			}

			if reply.Term > rf.CurrentTerm {
				rf.CurrentTerm = reply.Term
				rf.state = follower
				rf.VotedFor = -1
				rf.updateHeartBeat()
			} else if reply.VoteGranted {

				votesReceived++
				if votesReceived > len(rf.peers)/2 {
					rf.state = leader
					rf.leaderId = rf.me
					go rf.sendHeartBeats()
				}
			}

		}(i, rf.CurrentTerm, rf.me)
	}
}

func (rf *Raft) ticker() {
	for {
		electionTimeout := false
		var currentTerm, me int

		rf.mu.Lock()
		if time.Now().After(rf.electionDeadline) {
			if rf.state != leader {
				rf.state = candidate
				rf.CurrentTerm++
				rf.VotedFor = rf.me
				rf.updateHeartBeat()

				currentTerm, me = rf.CurrentTerm, rf.me
				electionTimeout = true
			}
		}
		rf.mu.Unlock()
		if electionTimeout {
			rf.startElection(currentTerm, me)
		}

		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.leaderId = -1
	rf.CurrentTerm = 0
	rf.VotedFor = -1
	rf.state = follower
	rf.updateHeartBeat()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
