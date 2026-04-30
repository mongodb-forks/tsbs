package main

import (
	"sync"
	"sync/atomic"
)

// loadErrState tracks whether any worker has hit a terminal write error.
// We deliberately avoid log.Fatalf from inside worker goroutines: that calls
// os.Exit immediately and prevents CommonBenchmarkRunner.postRun from printing
// the summary line ("N workers (mean rate X metrics/sec)") that downstream
// result parsers (e.g. DSI's TsbsLoadResultParser) rely on. Instead, workers
// record the first error here, return fast so RunBenchmark can shut down
// cleanly, and main() exits non-zero once the summary has been written.
var loadErrState struct {
	flag uint32
	mu   sync.Mutex
	err  error
}

func setLoadFailed(err error) {
	loadErrState.mu.Lock()
	if loadErrState.err == nil {
		loadErrState.err = err
	}
	loadErrState.mu.Unlock()
	atomic.StoreUint32(&loadErrState.flag, 1)
}

func loadFailed() bool {
	return atomic.LoadUint32(&loadErrState.flag) == 1
}

func loadError() error {
	loadErrState.mu.Lock()
	defer loadErrState.mu.Unlock()
	return loadErrState.err
}
