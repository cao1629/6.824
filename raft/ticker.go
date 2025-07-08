package raft

import (
    "math/rand"
    "sync"
    "time"
)

type TickerState int

const (
    Ready TickerState = iota
    Running
    Paused
    Stopped
)

type ElectionTicker struct {
    mu                 sync.Mutex
    electionTimeoutMin time.Duration
    electionTimeoutMax time.Duration
    timeoutChan        chan struct{}

    // Control channels
    pauseChan chan struct{}
    resetChan chan struct{}
    stopChan  chan struct{}

    // State
    state TickerState
}

// NewElectionTicker creates a new election ticker with specified timeout range
func NewElectionTicker(minTimeout, maxTimeout time.Duration) *ElectionTicker {
    return &ElectionTicker{
        electionTimeoutMin: minTimeout,
        electionTimeoutMax: maxTimeout,
        timeoutChan:        make(chan struct{}, 1),
        pauseChan:          make(chan struct{}, 1),
        resetChan:          make(chan struct{}, 1),
        stopChan:           make(chan struct{}, 1),
        state:              Ready,
    }
}

// generateRandomTimeout generates a random timeout between min and max
func (tk *ElectionTicker) generateRandomTimeout() time.Duration {
    if tk.electionTimeoutMin >= tk.electionTimeoutMax {
        return tk.electionTimeoutMin
    }
    diff := tk.electionTimeoutMax - tk.electionTimeoutMin
    return tk.electionTimeoutMin + time.Duration(rand.Int63n(int64(diff)))
}

// This method is only called once during the lifetime of an ElectionTicker.
// run() will be called concurrently.
func (tk *ElectionTicker) Start() {
    tk.state = Running
    go tk.run()
}

// Like Start(), Stop() could only be called once during the lifetime of an ElectionTicker.
// Therefore no need to provide synchronization.
func (tk *ElectionTicker) Stop() {
    tk.stopChan <- struct{}{}
}

// When do we pause a ticker? When a candidate is elected to be a leader.
func (tk *ElectionTicker) Pause() {
    tk.mu.Lock()
    defer tk.mu.Unlock()

    // The ticker has not started yet, or it is already stopped or paused.
    if tk.state != Running {
        return
    }

    // Send a pause signal to the pause channel. Because the channel is buffered with size 1,
    // we will not block here.
    tk.pauseChan <- struct{}{}
}

// When Reset() is called, the ticker is either running or paused.
func (tk *ElectionTicker) Reset() {
    tk.mu.Lock()
    defer tk.mu.Unlock()

    tk.resetChan <- struct{}{}
}

// Called by Start().
// Stop() and Pause() could be called while the ticker is running.
// run() is a long-running goroutine. Therefore, we cannot simply lock it from the beginning to the end.
func (tk *ElectionTicker) run() {
    defer func() {
        tk.state = Stopped
    }()

    for {

        // We cannot run a ticker that is stopped.
        if tk.state == Stopped {
            return
        }

        // If paused, wait for reset or stop
        if tk.state == Paused {
            select {
            case <-tk.resetChan:
                tk.state = Running
                // go to the next iteration to start a new tick
                continue
            case <-tk.stopChan:
                tk.state = Stopped
                return
            }
        }

        // Start a new tick with random timeout
        tk.state = Running
        timeout := tk.generateRandomTimeout()
        timer := time.NewTimer(timeout)

        select {
        case <-timer.C:
            tk.timeoutChan <- struct{}{}

        case <-tk.pauseChan:
            tk.state = Paused

        case <-tk.resetChan:
            continue
            // Continue to next iteration to start a new tick immediately

        case <-tk.stopChan:
            tk.state = Stopped
            return
        }
    }
}
