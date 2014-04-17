package raft
import (
	"time"
	"fmt"
	"strings"
    "encoding/json"
    "math/rand"
    //"encoding/gob"
    "io/ioutil"
    "strconv"
    "os"
    cluster "../cluster"
)


//state machine a KVstore
func statemachine(n *Node,query string,index int){
	var temp []string=strings.Split(query,"|")
	if temp[0]=="insert" || temp[0]=="update"{
		n.DB[index]=temp[1]
		dat:=LogEntry{Data:"Done"}
		n.Inbox()<-&dat
	}else if temp[0]=="delete"{
		delete(n.DB,index)
		dat:=LogEntry{Data:"Deleted"}
		n.Inbox()<-&dat
	}else if temp[0]=="view"{
		if n.DB[index]==""{
			dat:=LogEntry{Data:"Not_Found"}
			n.Inbox()<-&dat
		}else{
			dat:=LogEntry{Data:n.DB[index]}
			n.Inbox()<-&dat
		}
	}
}


//generates random number
func random(min, max int) int {
    rand.Seed(time.Now().Unix())
    return rand.Intn(max - min) + min
}
//log entries struct
type LogEntry struct{
   	// An index into an abstract 2^64 size array
   	Index  int
   	Term int
	Commitcount int
    // The data that was supplied to raft's inbox
    Data    interface{}
}

//actual content of log
type Logcontent struct{
	Term int
	Command string
}


/*RPC struct contains all 3 types of rpc appendentries, request and respince votes*/
type RPC struct{
	Term int //Term id
	Id int  //Id of the server i.e follower,candidate or leader
	LeaderId int 
	VotedId int //candidate to which vote is given
	Type string //RequestVote, ResponceVote ,Appendentries, AppendResponce
	
	LastLogIndex int //index of candidate's last log entry
	LastLogTerm int //term of candidate's last log entry
	Entries []LogEntry	//log entries to store (empty for heartbeat)
	LeaderCommit int 	//leader’s commitIndex
	Success bool
	Heartbeat int
}


//interface for raft
type Raft interface{
	Id() int
    Term()     int
    isLeader() bool
    Quit() 
    Start()
    Reset()	//resets the server node to follower
    
	Leader()    int

	// Mailbox for state machine layer above to send commands of any
	// kind, and to have them replicated by raft.  If the server is not
	// the leader, the message will be silently dropped.
	Outbox() chan *LogEntry

	//Mailbox for state machine layer above to receive commands. These
	//are guaranteed to have been replicated on a majority
	Inbox() chan *LogEntry
	
	Givemap() map[int]string // gives the content of map KVStore
	
	GiveLog() []LogEntry	//gives the content of the log 

	//Remove items from 0 .. index (inclusive), and reclaim disk
	//space. This is a hint, and there's no guarantee of immediacy since
	//there may be some servers that are lagging behind).

	//DiscardUpto(index int64)
}


type Node struct{
	Snode cluster.Server //server node
	CurrentTerm int	//latest Term
	VotedFor int		//voted for
	L bool		//isleader
	State string //follower, leader or candidate
	ElecTout	int 	//election timeout time in millisec
	HeartBT int		//heartbeat time in millisec
	Majority int //Majority value
	Close chan bool //channel by which raft node is closed
	ES int 		//electout start
	EN int		//electout end
	Cout chan *LogEntry 	//channel for output
   	Cin chan *LogEntry		//channel for input entries
   	LeaderId int 		//id of the leader
   	Log []LogEntry //log entries; each entry contains command for state machine, and term
   	CommitIndex int	//index of highest log entry known to be committed
   	LastApplied int
   	
   	NextIndex []int 	/*for each server, index of the next log entry
to send to that server (initialized to leader
last log index + 1)*/

   	MatchIndex []int	/*for each server, index of highest log entry
known to be replicated on server
(initialized to 0, increases monotonically)*/

	DB map[int]string
   	
}

type jsonobject struct {
    EToutStart int 
    EToutEnd int 
    HTout int 
    MajValue int
}

func (n Node) Givemap() map[int]string{
	return n.DB
}
func (n Node) GiveLog() []LogEntry{
	return n.Log
}


func (n Node) Outbox() chan *LogEntry{
	return n.Cout
}

func (n Node) Inbox() chan *LogEntry{
	return n.Cin
}
 
func (n Node) Term() int {
   return n.CurrentTerm
} 
func (n Node) Leader() int {
   return n.LeaderId
} 
func (n Node) Reset(){
   n.L=false
   n.State="Follower"
}
func (n Node) Id() int {
   return n.Snode.Pid()
} 
func (n Node) isLeader() bool {
   return n.L
} 

//quite the raft server node
func (n Node) Quit() {
	n.CurrentTerm = 0
	n.VotedFor =0
	n.L=false
	n.State="Follower"
	n.Close <- true
}

//starts the raft server node
func (n Node) Start() {	
    go start_ser(&n)
}

func Leader(n *Node){
	//if query com"strconv"es from client
	select {
		//after timeout of heartbeat send appendentries to all
       	case <- time.After(time.Duration(n.HeartBT)*time.Millisecond): 
			//sending empty AppendEntry i.e heartbeat
			var entry []LogEntry
			hb:=RPC{n.CurrentTerm,n.Snode.Pid(),n.Snode.Pid(),0,"AppendEntries",0,0,entry,n.CommitIndex,false,1}
			b, _ := json.Marshal(hb)
			n.Snode.Outbox()<-&cluster.Envelope{Pid:-1, Msg: b}
       		
		//if any query comes from the client
		case query:= <- n.Outbox():
				//replicate the query to all followers by broadcasting
				q1 := LogEntry{Term:n.CurrentTerm,Index:query.Index,Data:query.Data.(string),Commitcount:0}
				var entry []LogEntry
				entry =append(entry,q1)
				n.Log=append(n.Log,entry...)	
				APE:=RPC{n.CurrentTerm,n.Snode.Pid(),n.Snode.Pid(),0,"AppendEntries",n.Log[len(n.Log)-2].Index,n.Log[len(n.Log)-2].Term,entry,n.CommitIndex,false,0}
		   		b, _ := json.Marshal(APE)
		   		n.Snode.Outbox()<-&cluster.Envelope{Pid:-1, Msg: b}				
		//if any envelope comes from follower or other server nodes
   		case envelope := <- n.Snode.Inbox():
   			
       		//decoding the envelope
			var dat RPC
			json.Unmarshal(envelope.Msg.([]byte),&dat)
           	switch dat.Type {       		
				case "RequestVote":
					//if Term is greater than CurrentTerm, then sitdown and become follower
					if n.CurrentTerm < dat.Term{
						//println("Term ",n.CurrentTerm," Leader ",n.Snode.Pid()," received ReqV from ",dat.Id, " whose Term is ",dat.Term)
						n.CurrentTerm=dat.Term
						//no more leader
						n.L=false
						//convert to follower
						n.State="Follower"
					}
				case "AppendResponce":
					
					//setting the commit index and updating in state machine
					if n.Log[dat.LastLogIndex].Commitcount >=n.Majority{
						n.CommitIndex = dat.LastLogIndex
						query:=n.Log[dat.LastLogIndex].Data.(string)
						statemachine(n,query,n.Log[dat.LastLogIndex].Index)
						var entry []LogEntry
						hb:=RPC{n.CurrentTerm,n.Snode.Pid(),n.Snode.Pid(),0,"AppendEntries",0,0,entry,n.CommitIndex,false,1}
						b, _ := json.Marshal(hb)
						n.Snode.Outbox()<-&cluster.Envelope{Pid:-1, Msg: b}
					}
					
					//If last log index ≥ nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex 
					if dat.LastLogIndex >= n.NextIndex[dat.Id] {
						/*appendentry:=RPC{Term:n.CurrentTerm,Id:n.Snode.Pid(),LeaderId:n.Snode.Pid(),Type:"AppendEntries",Entries:n.Log[n.NextIndex[dat.Id]:len(n.Log)-1],LeaderCommit:n.CommitIndex}
				   		b, _ := json.Marshal(appendentry)
				   		n.Snode.Outbox()<-&cluster.Envelope{Pid:dat.Id, Msg: b}*/
				   		n.NextIndex[dat.Id]=dat.LastLogIndex +1				   		
					}
					//if success is false send again the entries
					if dat.Success==false{
						appendentry:=RPC{Term:n.CurrentTerm,Id:n.Snode.Pid(),LeaderId:n.Snode.Pid(),Type:"AppendEntries",Entries:n.Log[0:len(n.Log)-1],LeaderCommit:n.CommitIndex}
				   		b, _ := json.Marshal(appendentry)
				   		n.Snode.Outbox()<-&cluster.Envelope{Pid:dat.Id, Msg: b}
					}else{
						//increment the commitcount in entries
						n.Log[dat.LastLogIndex].Commitcount = n.Log[dat.LastLogIndex].Commitcount + 1
					}
					
				default:
					//if greater term sends anything then convert to follower  
					if n.CurrentTerm < dat.Term{
							n.CurrentTerm=dat.Term
							//no more leader
							n.L=false
							//convert to follower
							n.State="Follower"
					}
					//println("Term ",n.CurrentTerm," Leader ",n.Snode.Pid()," received Garbage RPC",dat.Type)
			} 
   	}
   	return 
}

func Follower(n *Node) {
	select {
		case _, err := <- n.Outbox():
			if err == false {
				println("Error in query")
			}else {
				//Error and give address of leader
				t := strconv.Itoa(n.Leader())
				dat:=LogEntry{Data:"Sorry Leader ID is "+ t}
				n.Inbox()<-&dat	
			}
		case envelope := <- n.Snode.Inbox(): 
			//decoding the envelope
			var dat RPC
			json.Unmarshal(envelope.Msg.([]byte),&dat)
           	switch dat.Type { 
				default:
					//println("Term ",n.CurrentTerm," Follower ",n.Snode.Pid()," received Garbage RPC ")
				//if request vote then see if already voted than simply send the id to which voted else check the current Term and give vote
				case "RequestVote":
					//if nil then give vote	
					if n.VotedFor==0{
						//give vote only if (lastTermV <= lastTermC) || (lastTermV == lastTermC) && (lastIndexV <= lastIndexC)
						if n.CurrentTerm < dat.Term{
							if n.Log[len(n.Log)-1].Term <= dat.LastLogTerm || n.Log[len(n.Log)-1].Term == dat.LastLogTerm && n.Log[len(n.Log)-1].Index <= dat.LastLogIndex {
					
								//set the current id
								//println("Term ",n.CurrentTerm," server ",n.Snode.Pid()," received ReqV from ",dat.Id," whose Term is ",dat.Term)
								n.CurrentTerm=dat.Term
								n.VotedFor=dat.Id
								//println("Term ",n.CurrentTerm," server ",n.Snode.Pid()," Voted for ",dat.Id," whose Term is ",dat.Term)
								//creating a responce vote envelope and sending back with the id whom the vote is given
								rv:=RPC{Term:n.CurrentTerm,Id:n.Snode.Pid(),VotedId:n.VotedFor,Type:"ResponceVote"}
								b, _ := json.Marshal(rv)
								n.Snode.Outbox()<-&cluster.Envelope{Pid:dat.Id, Msg:b}
							}
						}
					}else{
						//if Term is greater than CurrentTerm then only give vote
						if n.CurrentTerm < dat.Term{
							if n.Log[len(n.Log)-1].Term <= dat.LastLogTerm || n.Log[len(n.Log)-1].Term == dat.LastLogTerm && n.Log[len(n.Log)-1].Index <= dat.LastLogIndex {
					
								//set the current id
								//println("Term ",n.CurrentTerm," server ",n.Snode.Pid()," received ReqV from ",dat.Id," whose Term is ",dat.Term)
								n.CurrentTerm=dat.Term
								n.VotedFor=dat.Id
								//println("Term ",n.CurrentTerm," server ",n.Snode.Pid()," Voted for ",dat.Id," whose Term is ",dat.Term)
								//creating a responce vote envelope and sending back with the id whom the vote is given
								rv:=RPC{Term:n.CurrentTerm,Id:n.Snode.Pid(),VotedId:n.VotedFor,Type:"ResponceVote"}
								b, _ := json.Marshal(rv)
								n.Snode.Outbox()<-&cluster.Envelope{Pid:dat.Id, Msg:b}
							}
						}else{
							if n.CurrentTerm == dat.Term{
								//println("Term ",n.CurrentTerm," server ",n.Snode.Pid()," received ReqV ",dat.Id," whose Term is ",dat.Term)
								//println("Term ",n.CurrentTerm," server ",n.Snode.Pid()," Voted for ",n.VotedFor," whose Term is ",dat.Term)
								//creating a responce vote envelope and sending back with the id whom the vote is given
								rv:=RPC{Term:n.CurrentTerm,Id:n.Snode.Pid(),VotedId:n.VotedFor,Type:"ResponceVote"}
								b, _ := json.Marshal(rv)
								n.Snode.Outbox()<-&cluster.Envelope{Pid:dat.Id, Msg: b}					
							}
						}
					}
				//if appendenrty do nothing only set the current Term
				case "AppendEntries":	
					//updating the commitindex and log entry in follower's statemachine
					if dat.LeaderCommit > n.LastApplied{
						n.CommitIndex = dat.LeaderCommit
						if n.LastApplied==0{
							for i:=1;i<=dat.LeaderCommit;i++{
								query:=n.Log[i].Data.(string)
								var temp []string=strings.Split(query,"|")
								if temp[0]=="insert" || temp[0]=="update"{
									n.DB[n.Log[i].Index]=temp[1]
								}else if temp[0]=="delete"{
									delete(n.DB,n.Log[i].Index)
								}
							}
							n.LastApplied = dat.LeaderCommit						
						}else{
							for i:=n.LastApplied+1;i<=dat.LeaderCommit;i++{
								query:=n.Log[i].Data.(string)
								var temp []string=strings.Split(query,"|")
								if temp[0]=="insert" || temp[0]=="update"{
									n.DB[n.Log[i].Index]=temp[1]
								}else if temp[0]=="delete"{
									delete(n.DB,n.Log[i].Index)
								}
							}
							n.LastApplied = dat.LeaderCommit	
						}
					}	
					
					//if the message is heart beat just update the current term
					if dat.Term >= n.CurrentTerm && dat.Heartbeat==1{
						//println("Term ",n.CurrentTerm," server ",n.Snode.Pid()," received HEARTBEAT from ",dat.LeaderId," whose Term is ",dat.Term)
						n.CurrentTerm=dat.Term	//updating term
						n.LeaderId=dat.Id		//updating leader id
						n.CommitIndex= dat.LeaderCommit
					}else{
					//println("Term ",n.CurrentTerm," server ",n.Snode.Pid()," received AE from ",dat.LeaderId," whose Term is ",dat.Term)
					//if the message is not heartbeat 
						//if term is less ignore the RPC
						if dat.Term < n.CurrentTerm{
							//ignored
						}else if dat.LastLogIndex < len(n.Log) && n.Log[dat.LastLogIndex].Term != dat.LastLogTerm{
							//Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
							//the entry of follower and leader are not matching send the index where it is matching
							resappend:=RPC{Term:n.CurrentTerm,Id:n.Snode.Pid(),Type:"AppendResponce",Success:false,LastLogIndex:-1}
							b, _ := json.Marshal(resappend)
							n.Snode.Outbox()<-&cluster.Envelope{Pid:dat.Id, Msg: b}	
						}else{					
							//Append any new entries not already in the log
							n.Log = append(n.Log,dat.Entries...)
							//send respnce of APE to leader with success
							resappend:=RPC{Term:n.CurrentTerm,Id:n.Snode.Pid(),Type:"AppendResponce",Success:true,LastLogIndex:len(n.Log)-1}
							b, _ := json.Marshal(resappend)
							n.Snode.Outbox()<-&cluster.Envelope{Pid:dat.Id, Msg: b}	
							
							n.CurrentTerm=dat.Term	//updating term
							n.LeaderId=dat.Id		//updating leader id
							n.CommitIndex= dat.LeaderCommit
						}
						//if leaderCommit > commitIndex, set commitIndex = min(leaderCommit, last log index)
						if dat.LeaderCommit > n.CommitIndex{
							if dat.LeaderCommit < dat.LastLogIndex{
								n.CommitIndex = dat.LeaderCommit
							}else{
								n.CommitIndex = dat.LastLogIndex
							}
						}
					}
			}
		//if election timeout then become candidate		
       	case <- time.After(time.Duration(n.ElecTout + 2000) * time.Millisecond): 
       		n.ElecTout=random(n.ES,n.EN)
       		//println("Term ",n.CurrentTerm," server ",n.Snode.Pid()," became candidate because of timeout")
       		n.State="Candidate"
   	}
   	return 
}
func Candidate(n *Node){
	//increment the Term
	n.CurrentTerm=n.CurrentTerm+1
	//vote for self
	n.VotedFor=n.Snode.Pid()
	//send RV_RPC
	//println("Term ",n.CurrentTerm," candidate ",n.Snode.Pid()," sending ReqV")
	RV:=RPC{Term:n.CurrentTerm,Id:n.Snode.Pid(),VotedId:n.VotedFor,Type:"RequestVote"}
	b2, err1 := json.Marshal(RV)
	if err1 != nil {
		//println("candidate marshal error:", err1)
	}

    n.Snode.Outbox()<-&cluster.Envelope{Pid:-1, Msg: b2}
    var votes int
    votes=1
	for{
		//if Majority votes comes then the candidate became leader  
		if votes >=n.Majority{
				//Majority votes so become leader
				n.State="Leader"
				//println("Term ",n.CurrentTerm," candidate ",n.Snode.Pid()," became leader")
				n.L=true
				return
		}
		select {	
		   case envelope := <- n.Snode.Inbox(): 
		   		//decoding the envelope
				var dat RPC
				json.Unmarshal(envelope.Msg.([]byte),&dat)
				//fmt.Println(dat)
		       	switch dat.Type { 
					default:
						//println("Term ",n.CurrentTerm," Candidate ",n.Snode.Pid()," received Garbage RPC")
						return 
					case "RequestVote":
						//request came from same Term but diff candidate
						if n.CurrentTerm == dat.Term{
							//println("Term ",n.CurrentTerm," candidate ",n.Snode.Pid()," received ReqV from ",dat.Id," whose Term is ",dat.Term)
							//creating a responce vote envelope and sending back with the id whom the vote is given
							rv:=RPC{Term:n.CurrentTerm,Id:n.Snode.Pid(),VotedId:n.VotedFor,Type:"ResponceVote"}
							b, _ := json.Marshal(rv)
							n.Snode.Outbox()<-&cluster.Envelope{Pid:dat.Id, Msg:b}
						//if higher term requests for vote than convert to follower
						}else if n.CurrentTerm < dat.Term{
							//println("Term ",n.CurrentTerm," candidate ",n.Snode.Pid()," became follower")
							n.State="Follower"
							n.CurrentTerm=dat.Term
							return					
						}
					
					case "ResponceVote":
					//if anybody with same Term number voted then increment the count of vote
						if n.CurrentTerm==dat.Term{
							if dat.VotedId==n.Snode.Pid(){
								//println("Term ",n.CurrentTerm," candidate ",n.Snode.Pid()," received a vote ")
								votes=votes+1
							}
						}
					case "AppendEntries":
					//if any append entry with >= Term comes then someone has become leader so sit down
						if dat.Term >= n.CurrentTerm{
							//println("Term ",n.CurrentTerm," candidate ",n.Snode.Pid()," received AE from ",dat.LeaderId," whose Term is ",dat.Term)
							//println("Term ",n.CurrentTerm," candidate ",n.Snode.Pid()," became follower")
							n.State="Follower"
							n.CurrentTerm=dat.Term
							return
						}
				}			
				
				
		   case <- time.After(time.Duration(n.ElecTout) * time.Millisecond): 
		   		n.State="Candidate"

		   		n.ElecTout = random(n.ES,n.EN)
		   		//break
		   		return 
	   		}
   	}
   	return 
}

func New_raft(id int,maj int,clus_conf string,raft_conf string) *Node{
	//reading json file and storing values
	file, e := ioutil.ReadFile(raft_conf)
    if e != nil {
        fmt.Printf("File error: %v\n", e)
        os.Exit(1)
    } 
    var jsontype jsonobject
    json.Unmarshal(file,&jsontype)
    
    //fetching election_timeout heartbeat_timeout and Majority value
    ES := jsontype.EToutStart
    EN := jsontype.EToutEnd
    HB:=jsontype.HTout
    
    //making a server node
    SNode:=cluster.New(id,clus_conf)
    var entry []LogEntry
    dummyentry:=LogEntry{Index:0,Term:0}
    //appending dummy enrty
    entry=append(entry,dummyentry)
    var nextindex []int
    var matchindex []int
    
    //initializing matchindex to zero
    matchindex = append(matchindex,0,0,0,0,0,0)
    //initializing nextindex to 1
    nextindex = append(nextindex,0,0,0,0,0,0)
    
    mynode := Node{SNode,0,0,false,"Follower",random(ES,EN),HB,maj,make(chan bool),ES,EN,make(chan *LogEntry),make(chan *LogEntry),0,entry,0,0,nextindex,matchindex,make(map[int]string)}

    go start_ser(&mynode)
    return &mynode
}

func start_ser(n *Node){
	for{
			select {
				case msg := <-n.Close:
					if msg == true {
						n.L=false
						println("server ",n.Snode.Pid()," closed")
						return						
					}
				default:
					switch n.State {
						case "Follower":
							Follower(n)
						case "Leader":
							Leader(n)
						case "Candidate":
							Candidate(n)
						default:
								//println("unrecognized State")
						}
			}
	}
	return
}
