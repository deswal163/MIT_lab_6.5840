// Package dlog provides a shared, env-var driven logging facility for the
// 6.5840 labs. See docs/logging-guide.md for the full design rationale.
//
// Two environment variables control behaviour at runtime:
//
//	VERBOSE=1        turn logging on (default: off, zero cost)
//	LOG_TOPICS=...   comma-separated subset of topic codes; unset = all topics
//
// Each line emitted has the form:
//
//	<elapsed> <who> <topic> <message>
//
// where <elapsed> is monotonic time-since-process-start in units of 100µs,
// <who> is the per-process identity (e.g. "S0", "KC2", "MC"), and <topic> is
// a four-letter category from the Topic constants below.
package dlog

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Topic categorises a log line. Filterable via LOG_TOPICS.
type Topic string

const (
	Info Topic = "INFO"
	Warn Topic = "WARN"
	Erro Topic = "ERRO"
	Trce Topic = "TRCE"

	Vote Topic = "VOTE"
	Lead Topic = "LEAD"
	Term Topic = "TERM"
	Log1 Topic = "LOG1"
	Log2 Topic = "LOG2"
	Cmit Topic = "CMIT"
	Pers Topic = "PERS"
	Snap Topic = "SNAP"
	Timr Topic = "TIMR"
	Drop Topic = "DROP"

	Clnt Topic = "CLNT"
	KGet Topic = "KGET"
	KPut Topic = "KPUT"

	MCrd Topic = "MCRD"
	MTsk Topic = "MTSK"
	MWrk Topic = "MWRK"

	Shrd Topic = "SHRD"
	Ctrl Topic = "CTRL"

	Test Topic = "TEST"
	AE   Topic = "AE"
)

var (
	verbosity int
	enabled   map[Topic]bool
	startTime time.Time
	initOnce  sync.Once
)

func setup() {
	startTime = time.Now()
	verbosity = parseVerbosity()
	enabled = parseTopics()
	log.SetFlags(0)
}

func parseVerbosity() int {
	v := os.Getenv("VERBOSE")
	if v == "" {
		return 0
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0
	}
	return n
}

func parseTopics() map[Topic]bool {
	raw := os.Getenv("LOG_TOPICS")
	if raw == "" {
		return nil
	}
	m := make(map[Topic]bool)
	for _, t := range strings.Split(raw, ",") {
		t = strings.ToUpper(strings.TrimSpace(t))
		if t != "" {
			m[Topic(t)] = true
		}
	}
	return m
}

// Enabled reports whether logging is on. Use it as a fast guard before
// computing any arguments that would themselves be expensive (e.g. building
// the per-call "who" prefix via fmt.Sprintf).
func Enabled() bool {
	initOnce.Do(setup)
	return verbosity >= 1
}

// Printf emits a single log line if logging is enabled and the topic passes
// the LOG_TOPICS filter (if any). Returns immediately otherwise.
func Printf(topic Topic, who string, format string, args ...interface{}) {
	initOnce.Do(setup)
	if verbosity < 1 {
		return
	}
	if enabled != nil && !enabled[topic] {
		return
	}
	elapsed := time.Since(startTime).Microseconds() / 100
	prefix := fmt.Sprintf("%06d %s %s ", elapsed, who, topic)
	log.Printf(prefix+format, args...)
}
