package paxos

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

func (p *PaxosNode) DBG(formatString string, args ...interface{}) {
	DebugTrace.Output(2, fmt.Sprintf("DBG: [%v] %v", p.Id, fmt.Sprintf(formatString, args...)))
}
func (p *PaxosNode) INF(formatString string, args ...interface{}) {
	InfoTrace.Output(2, fmt.Sprintf("INF: [%v] %v", p.Id, fmt.Sprintf(formatString, args...)))
}
func (p *PaxosNode) ERR(formatString string, args ...interface{}) {
	ErrTrace.Output(2, fmt.Sprintf("ERR: [%v] %v", p.Id, fmt.Sprintf(formatString, args...)))
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

func (pckv *PaxosClientKV) DBG(formatString string, args ...interface{}) {
	ClientDebugTrace.Output(2, fmt.Sprintf("DBG:  %v", fmt.Sprintf(formatString, args...)))
}
func (pckv *PaxosClientKV) INF(formatString string, args ...interface{}) {
	ClientInfoTrace.Output(2, fmt.Sprintf("INF:  %v", fmt.Sprintf(formatString, args...)))
}
func (pckv *PaxosClientKV) ERR(formatString string, args ...interface{}) {
	ClientErrTrace.Output(2, fmt.Sprintf("ERR:  %v", fmt.Sprintf(formatString, args...)))
}
