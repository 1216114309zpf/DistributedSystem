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

import "sync"
import "labrpc"
import "time"
import "math/rand"
import "fmt"
import "bytes"
import "labgob"

const DEBUG = false 
func printf(format string, a ...interface{}) (n int, err error) {
        if DEBUG {
                fmt.Printf(format, a...)
        }
        return
}

// import "bytes"
// import "labgob"



//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
        Term         int
        Index        int
        Command      interface{}
}

const (
           FOLLOWER  = 0
           CANDIDATE = 1
           LEADER    = 2

           BASIC     = 300
           VARIATION = 100
           APPEND    = 140
      )

func min(a int, b int) int {
      if a < b {
          return a
      }
      return b
}
//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
        
        //persistent states on all servers.
        currentTerm int
        votedFor    int
        log         []LogEntry

        //valatile states on all servers
        //can only have three values:FOLLOWER 0, CANDIDATE 1, LEADER 2
        role        int
        commitIndex int
        lastApplied int
        electTimer  *time.Timer

        applyCh     chan ApplyMsg

        //valatile states on leaders
        nextIndex   []int
        matchIndex  []int
        appendTimer *time.Timer
}


// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool

	// Your code here (2A).
        // use a lock maybe
        rf.mu.Lock()
        defer rf.mu.Unlock()
        term = rf.currentTerm
        isleader = rf.role==LEADER

	return term, isleader
}


//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
        writer := new(bytes.Buffer)
        encoder := labgob.NewEncoder(writer)
        encoder.Encode(rf.currentTerm)
        encoder.Encode(rf.votedFor)
        encoder.Encode(rf.log)
        data := writer.Bytes()
        rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
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
        reader  := bytes.NewBuffer(data)
        decoder := labgob.NewDecoder(reader)
        var currentTerm int
        var votedFor    int
        var log         []LogEntry
        if decoder.Decode(&currentTerm) != nil ||
           decoder.Decode(&votedFor)    != nil ||
           decoder.Decode(&log)         != nil {
               fmt.Println("Error: decode the state failure!")
        }else{
               rf.currentTerm = currentTerm
               rf.votedFor    = votedFor
               rf.log         = log
        }
}


type AppendEntriesArgs struct {
      Term          int
      LeaderId      int
      PrevLogIndex  int
      PrevLogTerm   int
      Entries       []LogEntry
      LeaderCommit  int
}

type AppendEntriesReply struct {
      Term          int
      Success       bool
}

//
// AppendEntries RPC handler
//

func (rf *Raft) ResetElectTimer() {
      d := time.Duration(VARIATION * rand.Float64() + BASIC)
      if rf.electTimer==nil {
           rf.electTimer = time.NewTimer(d * time.Millisecond)
      }else{
           rf.electTimer.Reset(d * time.Millisecond)
      }
}

func (rf *Raft) ResetAppendTimer() {
      d := time.Duration(APPEND)
      if rf.appendTimer==nil {
           rf.appendTimer = time.NewTimer(d * time.Millisecond)
      }else{
           rf.appendTimer.Reset(d * time.Millisecond)
      }
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
       rf.mu.Lock()
       defer rf.mu.Unlock()

       if args.Term > rf.currentTerm {
            rf.currentTerm = args.Term
            rf.role = FOLLOWER
            rf.votedFor = -1
            rf.persist()
       }

       reply.Term = rf.currentTerm
       if args.Term < rf.currentTerm {
           reply.Success = false
           return 
       }else{ //get an AppendEntries from current leader
           if rf.role != LEADER {
               rf.role = FOLLOWER
               rf.ResetElectTimer()
           }
       }

       if len(rf.log) < args.PrevLogIndex+1 || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
           reply.Success = false
           return
       }

       reply.Success = true

       rf.AppendEntriesOnSuccess(args)
       return
}

func (rf *Raft) sendApplyMsg(entry LogEntry) {
       applyMsg := ApplyMsg{true, entry.Command, entry.Index}
       printf("Peer %d apply Command %d successfully\n", rf.me, entry.Command)
       rf.applyCh <- applyMsg
}
//
// AppendEntries when it passed the AppendEntries check
//
func (rf *Raft) AppendEntriesOnSuccess(args *AppendEntriesArgs) {

      for i:=0; i<len(args.Entries); i++ {
            if len(rf.log) < args.PrevLogIndex + 2 + i {
                 rf.log = append(rf.log, args.Entries[i])
                 continue 
            }

            if args.Entries[i].Term    != rf.log[args.PrevLogIndex+1+i].Term  ||
                     args.Entries[i].Index   != rf.log[args.PrevLogIndex+1+i].Index ||
                     args.Entries[i].Command != rf.log[args.PrevLogIndex+1+i].Command {
                 rf.log = rf.log[:args.PrevLogIndex + 1 + i]
                 rf.log = append(rf.log, args.Entries[i]) 
            }
      }

      rf.persist()

      if args.LeaderCommit > rf.commitIndex && rf.role != LEADER { 
           rf.commitIndex = min(args.LeaderCommit, args.PrevLogIndex + len(args.Entries))
      }

      for index := rf.lastApplied+1; index <= rf.commitIndex; index++ {
           rf.sendApplyMsg(rf.log[index])
      }

      for rf.commitIndex > rf.lastApplied {
            rf.lastApplied++
            //apply rf.log[rf.lastApplied] to state machine
      }
}


func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
        var theReply AppendEntriesReply
        var newArgs AppendEntriesArgs
        rf.mu.Lock()
        newArgs = *args
        rf.mu.Unlock()
        ok := rf.peers[server].Call("Raft.AppendEntries", &newArgs, &theReply)
        rf.mu.Lock()
        defer rf.mu.Unlock()
        *reply = theReply
        return ok
}

func (rf *Raft) sendAppendEntriesParallel() {
        //printf("Length of rf.log is %d\n",len(rf.log))
        var replys []AppendEntriesReply = make([]AppendEntriesReply, len(rf.peers))
        var args   []AppendEntriesArgs  = make([]AppendEntriesArgs, len(rf.peers))
        
        rf.mu.Lock()

        logLength := len(rf.log)

        for i:=0; i<len(rf.peers); i++ {
             serverNo:=i

             replys[serverNo].Term = -1
             replys[serverNo].Success = false

             args[serverNo].Term = rf.currentTerm
             args[serverNo].LeaderId = rf.me
             args[serverNo].PrevLogIndex = rf.nextIndex[serverNo] - 1
             args[serverNo].PrevLogTerm  = rf.log[args[serverNo].PrevLogIndex].Term
             args[serverNo].LeaderCommit = rf.commitIndex
             args[serverNo].Entries = nil
             if  rf.nextIndex[serverNo] < len(rf.log) {
                   for index:=rf.nextIndex[serverNo]; index<len(rf.log); index++ {
                        args[serverNo].Entries = append(args[serverNo].Entries, rf.log[index])
                   }
             }

             go rf.sendAppendEntries(serverNo,&args[serverNo],&replys[serverNo])
        }
        rf.mu.Unlock()

        <-rf.appendTimer.C

        rf.mu.Lock()
        defer rf.mu.Unlock()
        for i:=0; i<len(rf.peers); i++ {
                 if replys[i].Term <= 0 {//timeout before get the result of RPC call
                      continue
                 }

                 if replys[i].Term > rf.currentTerm {
                      printf("Peer %d 's term is more bigger than me\n",i)
                      rf.currentTerm = replys[i].Term
                      rf.votedFor = -1
                      rf.role = FOLLOWER
                      rf.persist()
                      return 
                 }else{
                     //update nextIndex and matchIndex of the leader if this is a real appendEntry instead of a heartbeat
                     if rf.nextIndex[i] < logLength {//a real appendEntry
                        if replys[i].Success {
                            rf.nextIndex[i] += len(args[i].Entries)
                            rf.matchIndex[i] = rf.nextIndex[i]-1
                            //rf.matchIndex[i] = rf.nextIndex[i]
                            //rf.nextIndex[i]++
                        }else{
                            //printf("Find decrement of nextINdex in real appendEntry, value of nextIndex[i] is %d now\n",rf.nextIndex[i]-1)
                            rejectTerm := rf.log[rf.nextIndex[i]].Term
                            for {
                               if rf.log[rf.nextIndex[i]].Term == rejectTerm && rf.nextIndex[i] != 1 {
                                     rf.nextIndex[i]--
                               }else{
                                     break
                               }
                            }
                            //rf.nextIndex[i]--
                        }
                    }else{//just an empty heartbeat
                        if !replys[i].Success {
                            rf.nextIndex[i]--
                            //printf("Find decrement of nextINdex in empty heartbeat, value of nextIndex[i] is %d now\n",rf.nextIndex[i])
                        }
                    }
                 }
        }


        for i:=0; i<len(rf.peers); i++ {
             if rf.matchIndex[i] <= rf.commitIndex {
                 continue
             }
             count:=0
             for j:=0; j<len(rf.peers); j++ {
                  if rf.matchIndex[j] >= rf.matchIndex[i] {
                      count++
                  }
             }
             if count > len(rf.peers)/2 && rf.log[rf.matchIndex[i]].Term == rf.currentTerm  {
                  rf.commitIndex = rf.matchIndex[i]
             }
        }
}



//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
        Term          int
        CandidateId   int
        LastLogTerm   int
        LastLogIndex  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
        Term          int
        VoteGranted   bool
}


//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
        // Your code here (2A, 2B).

        //maybe here we need a lock!
        rf.mu.Lock()
        defer rf.mu.Unlock()
        if args.Term > rf.currentTerm {
             rf.currentTerm = args.Term
             rf.role        = FOLLOWER
             rf.votedFor    = -1 
             rf.persist()
        }

        reply.Term = rf.currentTerm
        if args.Term < rf.currentTerm {
            reply.VoteGranted = false
            return 
        }else{
            if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && moreUpToDate(rf,args) {
                 reply.VoteGranted = true
                 rf.votedFor = args.CandidateId
                 rf.persist()
                 printf("Peer %d vote for peer %d, in term %d\n", rf.me, args.CandidateId, rf.currentTerm)
                 if rf.role == LEADER {
                      rf.role = FOLLOWER
                 }else if rf.role == CANDIDATE {
              
                 }else{
                      rf.ResetElectTimer()
                 }
            }else{
                 reply.VoteGranted = false
            }
        }
        return 
}


func moreUpToDate(me *Raft,candidate *RequestVoteArgs) bool {
      if candidate.LastLogTerm > me.log[len(me.log)-1].Term || 
         candidate.LastLogTerm == me.log[len(me.log)-1].Term && candidate.LastLogIndex >= me.log[len(me.log)-1].Index {
            return true    
      }

      return false
} 
//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
        var theReply RequestVoteReply
	ok := rf.peers[server].Call("Raft.RequestVote", args, &theReply)
        rf.mu.Lock()
        defer rf.mu.Unlock()
        *reply = theReply
	return ok
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
        rf.mu.Lock()
        defer rf.mu.Unlock()
        term = rf.currentTerm
        isLeader = rf.role == LEADER
        if isLeader {
           index = len(rf.log)
           entry := LogEntry{rf.currentTerm, index, command}
           rf.log = append(rf.log, entry)
           rf.persist()
        }
	return index, term, isLeader
}


//
//call this function when a peer transmits to FOLLOWER state, a peer also begins as a FOLLOWER
//
func (rf *Raft) Follower() {
      rf.mu.Lock()
      rf.role = FOLLOWER
      rf.ResetElectTimer()
      rf.mu.Unlock()
   
      <-rf.electTimer.C //wait for election timeout,
      
      rf.mu.Lock()
      rf.role = CANDIDATE
      rf.mu.Unlock()
      go rf.Candidate()
}

//
//call this function when a peer transmits to Leader state.
//
func (rf *Raft) Leader() {
      printf("Peer %d becomes leader in term %d\n",rf.me, rf.currentTerm)
      for i:=0; i<len(rf.peers); i++ {
              rf.nextIndex[i]  = len(rf.log)
              rf.matchIndex[i] = 0
      }
      for ;true; {
          rf.mu.Lock()
          if rf.role != LEADER {
              go rf.Follower()
              rf.mu.Unlock()
              break
          }
          rf.mu.Unlock()

          rf.ResetAppendTimer()
          //send AppendEntries parallelly and periodically
          rf.sendAppendEntriesParallel()

          //<-rf.appendTimer.C
 
      }
}

//
//call this function when a peer transmits to Candidate state.
//
func (rf *Raft) Candidate() {
     for ;true; {
         rf.mu.Lock()
         if rf.role != CANDIDATE {
              go rf.Follower()
              rf.mu.Unlock()
              break
         }
         
         rf.currentTerm++
         rf.votedFor = rf.me
         rf.ResetElectTimer()
         rf.persist()
         printf("Peer %d start a new election in term %d\n", rf.me, rf.currentTerm)
         rf.mu.Unlock()

         if rf.sendRequestVoteResult() {
             rf.mu.Lock()
             rf.role = LEADER
             rf.electTimer.Stop()
             go rf.Leader()
             rf.mu.Unlock()
             printf("Peer %d breaks now\n",rf.me)
             break
         }
         //<-rf.electTimer.C
      }
}

func (rf *Raft) sendRequestVoteResult() bool {
      originalTerm := rf.currentTerm 
      args := RequestVoteArgs{rf.currentTerm, rf.me, rf.log[len(rf.log)-1].Term,len(rf.log)-1}
      var replys []RequestVoteReply = make([]RequestVoteReply, len(rf.peers))

      for i:=0; i<len(rf.peers); i++ {
           serverNo:=i
           go rf.sendRequestVoteParallel(serverNo,&args, &replys[serverNo])
      }
   
      <-rf.electTimer.C
      flag := false
      votes := 0
      rf.mu.Lock()
      for i:=0; i<len(rf.peers); i++ {
               if replys[i].Term > rf.currentTerm {
                     rf.currentTerm = replys[i].Term
                     rf.votedFor    = -1
                     rf.role        = FOLLOWER
                     flag           = true
                     rf.persist()
               }

               if replys[i].VoteGranted && rf.currentTerm == originalTerm {
                    votes++
               }
      }
      rf.mu.Unlock()

      printf("Peer %d in Term %d get %d votes, total peers %d\n",rf.me, originalTerm, votes, len(rf.peers))
      if flag {
          return false
      }

      if votes > len(rf.peers)/2 {
           return true
      }

      return false
}

func (rf *Raft) sendRequestVoteParallel(server int,args *RequestVoteArgs, reply *RequestVoteReply) {
      rf.sendRequestVote(server, args, reply)
}


//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
        rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
        rf.currentTerm = 0
        rf.votedFor = -1
        rf.log = append(rf.log,LogEntry{0,0,0})//term,index,command
        rf.role = FOLLOWER

        rf.commitIndex = 0
        rf.lastApplied = 0

        rf.nextIndex  = make([]int, len(rf.peers))
        rf.matchIndex = make([]int, len(rf.peers))

        //begin as a follower
        go rf.Follower()
        

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	return rf
}
