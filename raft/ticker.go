package raft

import (
    "sync"
    "time"
)

type ElectionTicker struct {
    C     chan time.Time
    timer *time.Timer
    mu    sync.Mutex
    rf    *Raft
}

func NewElectionTicker(rf *Raft) *ElectionTicker {
    ticker := &ElectionTicker{
        C:  make(chan time.Time),
        mu: sync.Mutex{},
        rf: rf,
    }
    return ticker
}

func (et *ElectionTicker) Reset(interval time.Duration) {
    et.mu.Lock()
    defer et.mu.Unlock()
    if et.timer != nil {
        et.timer.Stop()
    }
    et.timer = time.NewTimer(interval)
    go func() {
        tick := <-et.timer.C
        et.C <- tick
    }()
}

func (et *ElectionTicker) Stop() {
    et.mu.Lock()
    defer et.mu.Unlock()

    if et.timer != nil {
        et.timer.Stop()
    }
}

// Why don't I reuse the same ticker for election and heartbeat?
// I would need to create a new ticker for each heartbeat.
type HeartbeatTicker struct {
    C       chan time.Time
    mu      sync.Mutex
    ticking bool
    ticker  *time.Ticker
    rf      *Raft
}

func NewHeartbeatTicker(rf *Raft) *HeartbeatTicker {
    heartbeat := &HeartbeatTicker{
        C:       make(chan time.Time),
        mu:      sync.Mutex{},
        ticker:  time.NewTicker(heartbeatInterval),
        ticking: false,
        rf:      rf,
    }

    go func() {
        for {
            select {
            case tick := <-heartbeat.ticker.C:
                heartbeat.mu.Lock()
                if heartbeat.ticking {
                    LOG(dTime, "S%d, Term: %d, Heartbeat ticks at %v",
                        heartbeat.rf.me, heartbeat.rf.currentTerm, tick.UnixMilli())
                    heartbeat.C <- tick
                }
                heartbeat.mu.Unlock()
            }
        }
    }()

    return heartbeat
}

func (ht *HeartbeatTicker) Pause() {
    ht.mu.Lock()
    defer ht.mu.Unlock()
    ht.ticking = false
}

func (ht *HeartbeatTicker) Resume() {
    ht.mu.Lock()
    defer ht.mu.Unlock()
    ht.ticking = true
}
