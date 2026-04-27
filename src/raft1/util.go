package raft

import (
	"fmt"

	"6.5840/dlog"
)

const Debug = false

func (rf *Raft) dlog(t dlog.Topic, format string, args ...interface{}) {
	if !dlog.Enabled() {
		return
	}
	dlog.Printf(t, fmt.Sprintf("S%d", rf.me), format, args...)
}

func DPrintf(format string, a ...interface{}) {
	if !dlog.Enabled() {
		return
	}
	dlog.Printf(dlog.Info, "?", format, a...)
}
