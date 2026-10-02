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

	"6.5840/dlog"
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
	commitIndex      int
	lastApplied      int
	nextIndex        []int
	matchIndex       []int
	log              []LogEntry
	applyCond        *sync.Cond

	// log
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
	isleader = rf.state == leader

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

	rf.dlog(dlog.Vote, "<- S%d RequestVote(T=%d, lli=%d, llt=%d) [myT=%d, votedFor=%d]",
		args.CandidateId, args.Term, args.LastLogIndex, args.LastLogTerm,
		rf.CurrentTerm, rf.VotedFor)

	reply.VoteGranted = false
	defer func() {
		reply.Term = rf.CurrentTerm
	}()

	if args.Term < rf.CurrentTerm {
		rf.dlog(dlog.Vote, "refused S%d: stale term (T=%d < myT=%d)", args.CandidateId, args.Term, rf.CurrentTerm)
		return
	}

	if rf.CurrentTerm < args.Term {
		rf.dlog(dlog.Term, "step down T=%d -> T=%d (RequestVote from S%d)", rf.CurrentTerm, args.Term, args.CandidateId)
		rf.VotedFor = -1
		rf.state = follower
		rf.CurrentTerm = args.Term
	}

	if rf.VotedFor != -1 && rf.VotedFor != args.CandidateId {
		rf.dlog(dlog.Vote, "refused S%d: already voted for S%d in T=%d", args.CandidateId, rf.VotedFor, rf.CurrentTerm)
		return
	}

	if rf.log[len(rf.log)-1].Term > args.LastLogTerm {
		rf.dlog(dlog.Drop, "refused S%d: stale term update (argsLTerm=%v, cTerm=%v)", args.CandidateId, rf.log[len(rf.log)-1].Term, args.LastLogTerm)
		return
	}

	if rf.log[len(rf.log)-1].Term == args.LastLogTerm && args.LastLogIndex < len(rf.log)-1 {
		rf.dlog(dlog.Drop, "refused S%d: logs not upto date (argsLIndex=%v, nLogs=%v)", args.CandidateId, args.LastLogIndex, len(rf.log)-1)
		return
	}

	rf.VotedFor = args.CandidateId
	rf.CurrentTerm = args.Term
	reply.Term = rf.CurrentTerm
	rf.state = follower
	reply.VoteGranted = true
	rf.updateHeartBeat()
	rf.dlog(dlog.Vote, "granted to S%d in T=%d", args.CandidateId, rf.CurrentTerm)
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
	LeaderCommit int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	defer func() {
		reply.Term = rf.CurrentTerm
	}()

	reply.Success = false

	if args.Term < rf.CurrentTerm {
		rf.dlog(dlog.Drop, "stale AE from S%d (argsT=%d < myT=%d)", args.LeaderId, args.Term, rf.CurrentTerm)
		return
	}

	wasLeader := rf.state == leader
	oldTerm := rf.CurrentTerm

	rf.leaderId = args.LeaderId
	rf.state = follower
	rf.updateHeartBeat()

	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.VotedFor = -1
		rf.dlog(dlog.Term, "step down T=%d -> T=%d (AE from S%d)", oldTerm, rf.CurrentTerm, args.LeaderId)
	}
	if wasLeader {
		rf.dlog(dlog.Lead, "leader stepping down: AE from S%d at argsT=%d", args.LeaderId, args.Term)
	}

	if len(rf.log)-1 < args.PrevLogIndex {
		rf.dlog(dlog.Drop, "Logs behind the S%v (nLog=%v, argsPIndex=%v)", args.LeaderId, len(rf.log)-1, args.PrevLogIndex)
		reply.ConflictIndex = len(rf.log) - 1
		return
	}

	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		rf.dlog(dlog.Drop, "Log Term mismatch from S%v (pTerm=%v, argsPTerm=%v) at index %v",
			args.LeaderId, rf.log[args.PrevLogIndex].Term, args.PrevLogTerm, args.PrevLogIndex)

		var i int
		for i = args.PrevLogIndex - 1; i >= 0 && rf.log[i].Term == rf.log[i+1].Term; i-- {
		}
		reply.ConflictIndex = i
		return
	}

	insertIndex := args.PrevLogIndex + 1
	rf.dlog(dlog.Trce, "<- S%v, inserting entries=%v from index %v", args.LeaderId, len(args.Entries), insertIndex)
	for i := 0; i < len(args.Entries) && insertIndex < len(rf.log); i++ {
		if rf.log[insertIndex].Term != args.Entries[i].Term {
			rf.dlog(dlog.Trce, "<- S%v, entries term mismatch — deleting entries from %v -> %v", args.LeaderId, insertIndex, len(rf.log)-1)
			rf.log = rf.log[:insertIndex]
			break
		}
		insertIndex++
	}

	if insertIndex <= args.PrevLogIndex+len(args.Entries) {
		rf.dlog(dlog.Trce, "<- S%v, appending the new entries from index %v (%v)", args.LeaderId, insertIndex, len(args.Entries[insertIndex-args.PrevLogIndex-1:]))
		rf.log = append(rf.log, args.Entries[insertIndex-args.PrevLogIndex-1:]...)
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.dlog(dlog.Cmit, "<- S%v, updating commit index (%v -> %v)", args.LeaderId, rf.commitIndex, min(args.LeaderCommit, len(rf.log)-1))
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
		rf.applyCond.Broadcast()
	}

	reply.Success = true
}

func (rf *Raft) convertToFollowerLocked() {
	rf.state = follower
	rf.VotedFor = -1
	rf.updateHeartBeat()
}

func (rf *Raft) broadcastAppendEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != leader {
		return
	}

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(peerID int) {
			rf.mu.Lock()

			lastLogIndex := len(rf.log) - 1

			prevIndex := rf.nextIndex[peerID] - 1
			args := AppendEntriesArgs{
				Term:         rf.CurrentTerm,
				LeaderId:     rf.me,
				LeaderCommit: rf.commitIndex,
				PrevLogIndex: prevIndex,
				PrevLogTerm:  rf.log[prevIndex].Term,
				Entries:      rf.log[prevIndex+1:],
			}
			rf.mu.Unlock()

			reply := AppendEntriesReply{}
			rf.dlog(dlog.AE, "-> S%v AppendEntries(T=%v, C=%v, pI=%v, pT=%v, nE=%v)",
				peerID, args.Term, args.LeaderCommit, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries))

			ok := rf.sendAppendEntries(peerID, &args, &reply)

			if !ok {
				rf.dlog(dlog.Drop, "Send AppendEntries failed for %d", peerID)
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			rf.dlog(dlog.AE, "<- S%v AE reply (rTerm=%v, rSuccess=%v)", peerID, reply.Term, reply.Success)

			if rf.state != leader {
				rf.dlog(dlog.Drop, "<- S%v AE, not the current leader", peerID)
				return
			}

			if reply.Term > rf.CurrentTerm {
				rf.dlog(dlog.Info, "<- S%v AE, Converting to follower received higher term", peerID)
				rf.CurrentTerm = reply.Term
				rf.convertToFollowerLocked()
				return
			}

			if !reply.Success {
				rf.dlog(dlog.Drop, "<- S%v AE, (success=%v, cTerm=%v, lTerm=%v, lIndex=%v)",
					peerID, reply.Success, rf.CurrentTerm, rf.log[lastLogIndex].Term, lastLogIndex)

				if rf.nextIndex[peerID] == args.PrevLogIndex+1 {
					rf.nextIndex[peerID] = max(1, reply.ConflictIndex)
				}
				return
			}

			rf.dlog(dlog.Info, "<- S%v AE, Log replicated update (mIndex=%v -> %v, nIndex=%v -> %v)",
				peerID, rf.matchIndex[peerID], lastLogIndex, rf.nextIndex[peerID], lastLogIndex+1)

			newMatch := args.PrevLogIndex + len(args.Entries)

			if newMatch > rf.matchIndex[peerID] {
				rf.matchIndex[peerID] = newMatch
				rf.nextIndex[peerID] = newMatch + 1
			}

			if rf.commitIndex < rf.matchIndex[peerID] && rf.log[rf.matchIndex[peerID]].Term == rf.CurrentTerm {
				matchedCount := 0
				for otherPeer, _ := range rf.matchIndex {
					if rf.matchIndex[otherPeer] >= rf.matchIndex[peerID] {
						matchedCount += 1
					}
					if matchedCount > len(rf.peers)/2 {
						rf.dlog(dlog.Info, "entry (i=%v, count=%v) replicated updating cIndex(%v -> %v)",
							rf.matchIndex[peerID], matchedCount, rf.commitIndex, rf.matchIndex[peerID])
						rf.commitIndex = rf.matchIndex[peerID]
						rf.applyCond.Broadcast()
						return
					}
				}
			}
		}(i)
	}

}

func (rf *Raft) sendHeartBeats() {

	for {
		rf.mu.Lock()
		if rf.state != leader {
			rf.mu.Unlock()
			return
		}

		rf.mu.Unlock()

		rf.broadcastAppendEntries()
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := len(rf.log)
	term := rf.CurrentTerm

	isLeader := rf.state == leader

	if !isLeader {
		return index, term, false
	}

	rf.dlog(dlog.Info, "Appending to Log (index=%d, cTerm=%d, isLeader=%t)", len(rf.log), rf.CurrentTerm, isLeader)
	rf.log = append(rf.log, LogEntry{
		Term:    rf.CurrentTerm,
		Command: command,
	})
	rf.matchIndex[rf.me] = len(rf.log) - 1

	go rf.broadcastAppendEntries()

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

		go func(i, currentTerm, candidateId, lastLogIndex, lastLogTerm int) {
			args := RequestVoteArgs{
				Term:         currentTerm,
				CandidateId:  candidateId,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			rf.dlog(dlog.Vote, "-> S%d RequestVote(T=%d)", i, currentTerm)

			reply := RequestVoteReply{}

			ok := rf.sendRequestVote(i, &args, &reply)
			if !ok {
				rf.dlog(dlog.Drop, "RequestVote -> S%d: rpc failed (network/dead)", i)
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			rf.dlog(dlog.Vote, "<- S%d reply T=%d granted=%v", i, reply.Term, reply.VoteGranted)

			if rf.state != candidate || rf.CurrentTerm != savedCurrentTerm {
				rf.dlog(dlog.Drop, "stale RequestVote reply from S%d (state=%d, T=%d, savedT=%d)",
					i, rf.state, rf.CurrentTerm, savedCurrentTerm)
				return
			}

			if reply.Term > rf.CurrentTerm {
				rf.dlog(dlog.Term, "stepping down: vote reply T=%d > myT=%d (from S%d)",
					reply.Term, rf.CurrentTerm, i)
				rf.CurrentTerm = reply.Term
				rf.state = follower
				rf.VotedFor = -1
				rf.updateHeartBeat()
			} else if reply.VoteGranted {

				votesReceived++
				if votesReceived > len(rf.peers)/2 {
					rf.state = leader
					rf.leaderId = rf.me

					for peerID, _ := range rf.peers {
						rf.nextIndex[peerID] = len(rf.log)
					}
					rf.matchIndex[rf.me] = len(rf.log) - 1
					rf.dlog(dlog.Lead, "won election with %d/%d votes at T=%d",
						votesReceived, len(rf.peers), rf.CurrentTerm)
					go rf.sendHeartBeats()
				}
			}

		}(i, rf.CurrentTerm, rf.me, len(rf.log)-1, rf.log[len(rf.log)-1].Term)
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

				rf.dlog(dlog.Timr, "election timeout fired")
				rf.dlog(dlog.Lead, "follower/candidate -> candidate, T=%d", rf.CurrentTerm)

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

func (rf *Raft) applier(applyCh chan raftapi.ApplyMsg) {

	for {
		rf.mu.Lock()

		for rf.commitIndex <= rf.lastApplied {
			rf.applyCond.Wait()
		}

		rf.dlog(dlog.Cmit, "commiting entries from %v -> %v", rf.lastApplied, rf.commitIndex)
		entriesToApply := make([]raftapi.ApplyMsg, 0)

		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			entriesToApply = append(entriesToApply, raftapi.ApplyMsg{
				CommandValid: true,
				Command:      rf.log[i].Command,
				CommandIndex: i,
			})
		}

		rf.lastApplied = rf.commitIndex

		rf.mu.Unlock()

		for _, applyMsg := range entriesToApply {
			applyCh <- applyMsg
		}
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
	rf.log = append(rf.log, LogEntry{Term: 0, Command: nil})
	rf.matchIndex = make([]int, len(rf.peers))
	rf.nextIndex = make([]int, len(rf.peers))
	rf.updateHeartBeat()
	rf.applyCond = sync.NewCond(&rf.mu)
	go rf.applier(applyCh)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.dlog(dlog.Info, "started, T=%d, votedFor=%d", rf.CurrentTerm, rf.VotedFor)

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
