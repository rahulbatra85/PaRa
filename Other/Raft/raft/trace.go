package raft

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
)

var DebugTrace *log.Logger
var InfoTrace *log.Logger
var ErrTrace *log.Logger

func InitTracers() {
	DebugTrace = log.New(ioutil.Discard, "", log.Ltime|log.Lshortfile)
	InfoTrace = log.New(os.Stdout, "", log.Ltime|log.Lshortfile)
	//	InfoTrace = log.New(ioutil.Discard, "", log.Ltime|log.Lshortfile)
	ErrTrace = log.New(os.Stdout, "", log.Ltime|log.Lshortfile)
}

func SetDebugTrace(enable bool) {
	if enable {
		DebugTrace = log.New(os.Stdout, "", log.Ltime|log.Lshortfile)
	} else {
		DebugTrace = log.New(ioutil.Discard, "", log.Ltime|log.Lshortfile)
	}
}

func SetInfoTrace(enable bool) {
	if enable {
		InfoTrace = log.New(os.Stdout, "", log.Ltime|log.Lshortfile)
	} else {
		InfoTrace = log.New(ioutil.Discard, "", log.Ltime|log.Lshortfile)
	}
}

func (r *RaftNode) DBG(formatString string, args ...interface{}) {
	DebugTrace.Output(2, fmt.Sprintf("DBG: [%v] %v", r.Id, fmt.Sprintf(formatString, args...)))
}
func (r *RaftNode) INF(formatString string, args ...interface{}) {
	InfoTrace.Output(2, fmt.Sprintf("INF: [%v] %v", r.Id, fmt.Sprintf(formatString, args...)))
}
func (r *RaftNode) ERR(formatString string, args ...interface{}) {
	ErrTrace.Output(2, fmt.Sprintf("ERR: [%v] %v", r.Id, fmt.Sprintf(formatString, args...)))
}

///////////////////////////////////
//Node Manager
///////////////////////////////////

var NodeManagerDebugTrace *log.Logger
var NodeManagerInfoTrace *log.Logger
var NodeManagerErrTrace *log.Logger

func NodeMgrInitTracers() {
	NodeManagerDebugTrace = log.New(ioutil.Discard, "", log.Ltime|log.Lshortfile)
	NodeManagerInfoTrace = log.New(os.Stdout, "", log.Ltime|log.Lshortfile)
	NodeManagerErrTrace = log.New(os.Stdout, "", log.Ltime|log.Lshortfile)
}

func NodeMgrSetDebugTrace(enable bool) {
	if enable {
		NodeManagerDebugTrace = log.New(os.Stdout, "", log.Ltime|log.Lshortfile)
	} else {
		NodeManagerDebugTrace = log.New(ioutil.Discard, "", log.Ltime|log.Lshortfile)
	}
}

func NodeMgrSetInfoTrace(enable bool) {
	if enable {
		NodeManagerInfoTrace = log.New(os.Stdout, "", log.Ltime|log.Lshortfile)
	} else {
		NodeManagerInfoTrace = log.New(ioutil.Discard, "", log.Ltime|log.Lshortfile)
	}
}

func (nm *NodeManager) DBG(formatString string, args ...interface{}) {
	NodeManagerDebugTrace.Output(2, fmt.Sprintf("DBG:  %v", fmt.Sprintf(formatString, args...)))
}
func (nm *NodeManager) INF(formatString string, args ...interface{}) {
	NodeManagerInfoTrace.Output(2, fmt.Sprintf("INF:  %v", fmt.Sprintf(formatString, args...)))
}
func (p *NodeManager) ERR(formatString string, args ...interface{}) {
	NodeManagerErrTrace.Output(2, fmt.Sprintf("ERR:  %v", fmt.Sprintf(formatString, args...)))
}

///////////////////////////////////
//Client
///////////////////////////////////

var ClientDebugTrace *log.Logger
var ClientInfoTrace *log.Logger
var ClientErrTrace *log.Logger

func ClientInitTracers() {
	ClientDebugTrace = log.New(ioutil.Discard, "", log.Ltime|log.Lshortfile)
	ClientInfoTrace = log.New(os.Stdout, "", log.Ltime|log.Lshortfile)
	ClientErrTrace = log.New(os.Stdout, "", log.Ltime|log.Lshortfile)
}

func ClientSetDebugTrace(enable bool) {
	if enable {
		ClientDebugTrace = log.New(os.Stdout, "", log.Ltime|log.Lshortfile)
	} else {
		ClientDebugTrace = log.New(ioutil.Discard, "", log.Ltime|log.Lshortfile)
	}
}

func ClientSetInfoTrace(enable bool) {
	if enable {
		ClientInfoTrace = log.New(os.Stdout, "", log.Ltime|log.Lshortfile)
	} else {
		ClientInfoTrace = log.New(ioutil.Discard, "", log.Ltime|log.Lshortfile)
	}
}

func (rc *RaftClient) DBG(formatString string, args ...interface{}) {
	ClientDebugTrace.Output(2, fmt.Sprintf("DBG:  %v", fmt.Sprintf(formatString, args...)))
}
func (rc *RaftClient) INF(formatString string, args ...interface{}) {
	ClientInfoTrace.Output(2, fmt.Sprintf("INF:  %v", fmt.Sprintf(formatString, args...)))
}
func (rc *RaftClient) ERR(formatString string, args ...interface{}) {
	ClientErrTrace.Output(2, fmt.Sprintf("ERR:  %v", fmt.Sprintf(formatString, args...)))
}
