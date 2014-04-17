package raft
import (
	"time"
	//"fmt"
    //"math/rand"
    "testing"
    //"encoding/gob"
)

//set the majority value 
var MAJ int = 3

func Test_raft(t *testing.T){
	//making Raft servers and putting in array
   	var raft_arr []Raft
   	//pid starts from 1-5
  	p:=1
  	//starting all 5 raft servers
  	for i:=1;i<6;i++{
   		raft_arr=append(raft_arr,New_raft(p,MAJ,"peers.json","raft_conf.json"))
   		p++
   	}
   	
   	<-time.After(4*time.Second)
   	
   	var numofLeader = 0
   	var isleader = 0
   	//var quitted = 0
   	//var term = 0
   	////////////////////////////////////////////////////////////////////////////////////////////////
   	//Test 1 -checking only 1 leader is selected
   	for i:=0;i<5;i++{
   		if raft_arr[i].isLeader() {
   			isleader = i
   			numofLeader++
   			//term = raft_arr[i].Term()
   		}
   	}
   	//if there is only one leader test passed
   	if numofLeader !=1{
   		t.Errorf("Test 1 failed ")
   	}
   	
   		
   	///////////////////////////////////////////////////////////
   	//Test -2 sending insert query and checking it is inserted or not
   	query:=LogEntry{Index:1,Data:"insert|hello"}
	//broadcast the message
	raft_arr[isleader].Outbox()<-&query
	<-time.After(2*time.Second)
	
	var msg string
	select {
       case log := <- raft_arr[isleader].Inbox(): 
           msg =log.Data.(string)
       case <- time.After(5 * time.Second): 
           println("Error while inserting")
   	}
	
	if msg!="Done"{
		t.Errorf("Test 2 failed ")
	}
	
	
	///////////////////////////////////////////////////////////////////////
   	//Test -3 sending view query and checking the previous inserted query
   	query =LogEntry{Index:1,Data:"view|"}
	//broadcast the message
	raft_arr[isleader].Outbox()<-&query
	<-time.After(2*time.Second)
	select {
       case log := <- raft_arr[isleader].Inbox(): 
           msg =log.Data.(string)
       case <- time.After(5 * time.Second): 
           println("Error while inserting")
   	}
   	//check the value at index by printing the msg
   	//println(msg)
   	
   	
   	
   	//////////////////////////////////////////////////////////////////////
   	//Test -4 sending update query and checking it is inserted or not
   	query =LogEntry{Index:1,Data:"update|raju"}
	//broadcast the message
	raft_arr[isleader].Outbox()<-&query
	<-time.After(2*time.Second)
	
	select {
       case log := <- raft_arr[isleader].Inbox(): 
           msg =log.Data.(string)
       case <- time.After(5 * time.Second): 
           println("Error while inserting")
   	}
	//check the updated value at index by printing the msg
   	//println(msg)
	if msg!="Done"{
		t.Errorf("Test 4 failed ")
	}
	
	//////////////////////////////////////////////////////////////////////////
   	//Test 5 - sending delete query and checking it is deleted or not
  	
   	//shutting down the leader
   	//raft_arr[3].Quit()
   	<-time.After(2*time.Second)
   	query =LogEntry{Index:2,Data:"delete|"}
	//broadcast the message
	raft_arr[isleader].Outbox()<-&query
	<-time.After(2*time.Second)
	
	select {
       case log := <- raft_arr[isleader].Inbox(): 
           msg =log.Data.(string)
       case <- time.After(5 * time.Second): 
           println("Error while inserting")
   	}
	
	if msg!="Deleted"{
		t.Errorf("Test 5 failed ")
	}
   	//raft_arr[3].Start()
   	
   	
   	//////////////////////////////////////////////////////////////////////////////////////////
	
   	<-time.After(2*time.Second)
   	//printing the map of all servers 
   	//println("Map of All Servers")
	for i:=0;i<5;i++{
   		//fmt.Println("Server-",i+1,"Map",raft_arr[i].Givemap())
   	}
   	
   	//printing the log of all servers 
   	//println("Log of All Servers")
   	for i:=0;i<5;i++{
   		//fmt.Println("Server-",i+1,"Log",raft_arr[i].GiveLog())
   	}
   return
}
