package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"bytes"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Entry struct {
	Command interface{}
	Term    int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// 2A
	state       int  // 0: follower,  1: candidate,   2: leader
	resetTimer  bool // whether reset the election timeout timer
	votes       int  // votes received as a candidate in current term
	currentTerm int
	votedFor    int
	log         []Entry
	commitIndex int
	lastApplied int
	// if a leader
	nextIndex  []int
	matchIndex []int

	//2B
	// notifyApply chan bool     // channel to notify the applyCommit goroutine : 1. raft server closed , or 2. commitIndex updated
	applyCh chan ApplyMsg // channel to reply applied msg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = (rf.state == 2)
	rf.mu.Unlock()
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
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	raftState := w.Bytes()
	rf.persister.Save(raftState, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, votedFor int
	var logs []Entry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil {
		log.Fatalf("Decode fails in readPersist!")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = logs
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// -------------------------------- AppendEntries RPC ----------------------------------------------
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	// for fast backup
	XTerm  int
	XIndex int
	XLen   int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.checkHigherTerm(args.Term)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	func() { // here rf.currentTerm == args.Term  // candidate convert to follower
		if rf.state == 1 {
			rf.state = 0
			go rf.followerTicker()
		}
	}()
	rf.resetTimer = true
	reply.Term = rf.currentTerm
	// 2B
	if args.PrevLogIndex != 0 && // AppendEntriesRPC. 2 Rule
		(len(rf.log) < args.PrevLogIndex || rf.log[args.PrevLogIndex-1].Term != args.PrevLogTerm) {
		reply.Success = false
		if len(rf.log) < args.PrevLogIndex { // fast backup
			reply.XTerm = -1
			reply.XLen = len(rf.log)
		} else {
			reply.XTerm = rf.log[args.PrevLogIndex-1].Term
			idx := args.PrevLogIndex - 1
			for {
				if idx >= 1 && rf.log[idx-1].Term == reply.XTerm {
					idx--
				} else {
					break
				}
			}
			reply.XIndex = idx + 1
		}
		return
	}

	reply.Success = true
	myLogIndex := args.PrevLogIndex // drop conflicting entries, append new ones.     3. 4. Rules
	entryIndex := 0
	for {
		if len(rf.log) < myLogIndex+1 || entryIndex >= len(args.Entries) ||
			rf.log[myLogIndex].Term != args.Entries[entryIndex].Term {
			break
		}
		myLogIndex++
		entryIndex++
	}
	if entryIndex < len(args.Entries) {
		rf.log = rf.log[:myLogIndex]
		rf.log = append(rf.log, args.Entries[entryIndex:]...)
		rf.persist() // persists
	}

	if len(rf.log) < args.LeaderCommit { // update commit index, min(LeaderCommit, len(log))  ,5 Rule
		rf.commitIndex = len(rf.log)
	} else {
		rf.commitIndex = args.LeaderCommit
	}

}

func (rf *Raft) makeAppendEntriesArgs(sid int) *AppendEntriesArgs {
	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: rf.nextIndex[sid] - 1,
		PrevLogTerm:  -1,
		LeaderCommit: rf.commitIndex,
	}
	if args.PrevLogIndex != 0 {
		args.PrevLogTerm = rf.log[args.PrevLogIndex-1].Term
	}
	tocpyEntries := rf.log[rf.nextIndex[sid]-1:]
	args.Entries = make([]Entry, len(tocpyEntries))
	copy(args.Entries, tocpyEntries)
	return args
}

func (rf *Raft) sendAppendEntries(sid int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	oldTerm := args.Term
	ok := rf.peers[sid].Call("Raft.AppendEntries", args, reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.currentTerm != oldTerm || rf.state != 2 {
			return
		}
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.votes = 0
			rf.state = 0
			rf.nextIndex = []int{}
			rf.matchIndex = []int{}
			rf.persist() // persist
			go rf.followerTicker()
			return
		}
		if reply.Success {
			idx := args.PrevLogIndex + len(args.Entries)
			if idx > rf.matchIndex[sid] { // matchIndex[sid] may have been updated to a later position because of unordered replies
				rf.matchIndex[sid] = idx
				rf.nextIndex[sid] = idx + 1
			}
		} else {
			if reply.XTerm == -1 { //fast backup to find a proper nextIndex
				rf.nextIndex[sid] = reply.XLen + 1
			} else {
				var XTermIdx int
				var findXTerm bool = false
				for idx := args.PrevLogIndex - 1; idx >= 0; idx-- {
					if rf.log[idx].Term < reply.XTerm {
						break
					} else if rf.log[idx].Term == reply.XTerm {
						XTermIdx = idx
						findXTerm = true
						break
					}
				}
				if !findXTerm {
					rf.nextIndex[sid] = reply.XIndex
				} else {
					rf.nextIndex[sid] = XTermIdx + 1 + 1
				}
			}
			retryArgs := rf.makeAppendEntriesArgs(sid) // send a new AppendEntriesRPC immediately
			retryReply := &AppendEntriesReply{}
			go rf.sendAppendEntries(sid, retryArgs, retryReply)
		}
	}
}

// -------------------------------- RequestVote RPC ----------------------------
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.checkHigherTerm(args.Term)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}
	// here rf.votedFor == args.CandidateId is for the case that rf crashed and goes back again,
	// and it will read votedFor from persistent state
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		if len(rf.log) == 0 {
			reply.VoteGranted = true
			rf.resetTimer = true
			rf.votedFor = args.CandidateId
			rf.persist() // persist
			return
		}
		if rf.log[len(rf.log)-1].Term < args.LastLogTerm ||
			(rf.log[len(rf.log)-1].Term == args.LastLogTerm &&
				len(rf.log) <= args.LastLogIndex) {
			reply.VoteGranted = true
			rf.resetTimer = true
			rf.votedFor = args.CandidateId
			rf.persist() // persist
			return
		}
	}
	reply.VoteGranted = false
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	oldTerm := args.Term
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if (rf.currentTerm != oldTerm) || rf.state != 1 {
			return
		}
		if reply.Term > rf.currentTerm {
			rf.state = 0
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.votes = 0
			rf.persist() //persist
			go rf.followerTicker()
			return
		}
		if reply.VoteGranted {
			rf.votes += 1
			if rf.votes >= len(rf.peers)/2+1 {
				rf.state = 2
				rf.persist() //persist
				go rf.leaderTicker()
				return
			}
		}
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	if rf.killed() {
		index = -1
		term = -1
		isLeader = false
		return index, term, isLeader
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isLeader = (rf.state == 2)
	if !isLeader {
		index = -1
	} else {
		index = len(rf.log) + 1
		rf.log = append(rf.log, Entry{
			Term:    term,
			Command: command,
		})
		rf.persist() // persist
		// log.Printf("leader %v get a new log with index %v in term %v", rf.me, index, rf.currentTerm)
	}
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	// go func() { // use a goroutine to send kill msg, to avoid blocking and make Kill() return immediately
	// 	rf.notifyApply <- true
	// 	close(rf.notifyApply)
	// }()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// ----------------------- common functions for raft servers ---------------------------------------------------------
// goroutines for every raft server, to send ApplyMessage if commitIndex > lastApplied
func (rf *Raft) applyCommit() {
	for !rf.killed() {
		tmpMsgs := []ApplyMsg{}
		rf.mu.Lock()
		if rf.commitIndex > rf.lastApplied {
			for logId := rf.lastApplied - 1 + 1; logId <= rf.commitIndex-1; logId++ {
				tmpMsgs = append(tmpMsgs, ApplyMsg{
					CommandValid: true,
					Command:      rf.log[logId].Command,
					CommandIndex: logId + 1,
				})
				// log.Printf("server %v (state: %v) apply the msg(%v)", rf.me, rf.state, logId+1)
			}
			rf.lastApplied = rf.commitIndex
		}
		rf.mu.Unlock()
		for _, msg := range tmpMsgs { // without holding lock,  to avoid applyCh blocking
			// log.Printf("start msg %v!", msg.CommandIndex)
			rf.applyCh <- msg
			// log.Printf("success msg %v!", msg.CommandIndex)
		}
		time.Sleep(30 * time.Millisecond)
	}
}

func (rf *Raft) checkHigherTerm(term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.votedFor = -1
		rf.persist() //persist
		rf.votes = 0
		if rf.state != 0 {
			rf.state = 0
			go rf.followerTicker()
		}
	}
}

// ------------------------ ticker goroutines for different server state ---------
func (rf *Raft) followerTicker() {
	// rf.mu.Lock()
	// rf.resetTimer = false
	// rf.mu.Unlock()
	for !rf.killed() {
		ms := 100 + (rand.Int() % 200)
		time.Sleep(time.Duration(ms) * time.Millisecond)
		rf.mu.Lock()
		if rf.resetTimer {
			rf.resetTimer = false
			rf.mu.Unlock()
		} else {
			rf.mu.Unlock()
			go rf.candidateTicker()
			break
		}
	}
}

func (rf *Raft) candidateTicker() {
	for !rf.killed() {

		rf.mu.Lock()
		// log.Printf("server %v become the candidate in term %v", rf.me, rf.currentTerm)
		rf.state = 1
		rf.currentTerm += 1
		rf.votedFor = rf.me
		rf.persist() // persist
		rf.votes = 1
		rf.resetTimer = true
		term := rf.currentTerm
		lastLogIndex := len(rf.log)
		lastLogTerm := -1
		if lastLogIndex != 0 {
			lastLogTerm = rf.log[lastLogIndex-1].Term
		}
		rf.mu.Unlock()

		// send request vote rpcs to all other servers
		args := RequestVoteArgs{
			Term:         term,
			CandidateId:  rf.me,
			LastLogIndex: lastLogIndex,
			LastLogTerm:  lastLogTerm,
		}
		for sid := range rf.peers {
			if sid != rf.me {
				reply := RequestVoteReply{}
				go rf.sendRequestVote(sid, &args, &reply)
			}
		}

		ms := 100 + (rand.Int() % 200)
		time.Sleep(time.Duration(ms) * time.Millisecond)

		rf.mu.Lock()
		if rf.state != 1 {
			rf.mu.Unlock()
			break
		}
		rf.mu.Unlock()
	}
}

// leader's goroutine that periodically check lastLogIndex >= nextIndex
// and send AppendEntriesRPC
func (rf *Raft) leaderAppendEntriesTicker() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state != 2 {
			rf.mu.Unlock()
			return
		}
		for rid := range rf.peers {
			if rid != rf.me && len(rf.log) >= rf.nextIndex[rid] {
				args := rf.makeAppendEntriesArgs(rid)
				reply := &AppendEntriesReply{}
				go rf.sendAppendEntries(rid, args, reply)
			}
		}
		rf.mu.Unlock()
		time.Sleep(30 * time.Millisecond)
	}
}

// leader's goroutine to periodically update the commit index
func (rf *Raft) leaderUpdateCommitTicker() {
	for !rf.killed() {
		rf.mu.Lock()
		// oldCommit := rf.commitIndex
		if rf.state != 2 {
			rf.mu.Unlock()
			return
		}
		for n := rf.commitIndex + 1; n <= len(rf.log); n++ {
			if rf.log[n-1].Term != rf.currentTerm {
				continue
			}
			num := 1
			for pid := range rf.peers {
				if pid != rf.me && rf.matchIndex[pid] >= n {
					num++
				}
				if num >= len(rf.peers)/2+1 {
					rf.commitIndex = n
					break
				}
			}
		}
		// if oldCommit != rf.commitIndex {
		// 	// log.Printf("leader %v update the CommitIndex to %v in term %v", rf.me, rf.commitIndex, rf.currentTerm)
		// }
		rf.mu.Unlock()
		time.Sleep(20 * time.Millisecond)
	}
}

// leader's goroutine for initializing,  sending heart beat, and starting other goroutines
func (rf *Raft) leaderTicker() {
	rf.mu.Lock()
	if rf.state != 2 {
		rf.mu.Unlock()
		return
	}
	rf.nextIndex = make([]int, len(rf.peers)) // initialize matchIndex[] and nextIndex
	rf.matchIndex = make([]int, len(rf.peers))
	for rid := range rf.peers {
		if rid != rf.me {
			rf.nextIndex[rid] = len(rf.log) + 1
			rf.matchIndex[rid] = 0
		}
	}
	// log.Printf("server %v become the leader in term %v", rf.me, rf.currentTerm)
	rf.mu.Unlock()

	go rf.leaderAppendEntriesTicker()
	go rf.leaderUpdateCommitTicker()

	for !rf.killed() {
		rf.mu.Lock()
		if rf.state != 2 {
			rf.mu.Unlock()
			return
		}
		for rid := range rf.peers {
			if rid != rf.me {
				args := rf.makeAppendEntriesArgs(rid)
				reply := &AppendEntriesReply{}
				go rf.sendAppendEntries(rid, args, reply)
			}
		}
		rf.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
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
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.mu = sync.Mutex{}
	rf.dead = 0
	rf.state = 0
	rf.resetTimer = false
	rf.votes = 0
	rf.votedFor = -1
	rf.currentTerm = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start follower ticker goroutine to start elections
	go rf.followerTicker()
	// start the applyCommit goroutine, necessary for every raft server
	go rf.applyCommit()

	return rf
}
