package raft

import (
    "fmt"
    "testing"
    "time"
)

// Test basic ticker functionality
func TestElectionTicker_BasicTimeout(t *testing.T) {
    minTimeout := 10 * time.Second
    maxTimeout := 10 * time.Second
    ticker := NewElectionTicker(minTimeout, maxTimeout)

    // Start the ticker
    ticker.Start()
    start := time.Now()
    defer ticker.Stop()

    timeoutChan := ticker.GetTimeoutChan()

    go func() {
        time.Sleep(5 * time.Second) // Sleep for 5 seconds before resetting
        ticker.Reset()
    }()

    for {
        select {
        case <-timeoutChan:
            elapsed := time.Since(start)
            fmt.Printf("timeout: %s elapsed\n", elapsed)
        }
    }
}

// Test that timeouts are random within the specified range
func TestElectionTicker_RandomTimeout(t *testing.T) {
    minTimeout := 50 * time.Millisecond
    maxTimeout := 150 * time.Millisecond
    ticker := NewElectionTicker(minTimeout, maxTimeout)

    ticker.Start()
    defer ticker.Stop()

    // Collect multiple timeout durations
    timeouts := make([]time.Duration, 5)
    for i := 0; i < 5; i++ {
        start := time.Now()
        select {
        case <-ticker.GetTimeoutChan():
            timeouts[i] = time.Since(start)
        case <-time.After(maxTimeout + 100*time.Millisecond):
            t.Fatal("Timeout did not occur within expected time")
        }
    }

    // Check that all timeouts are within range
    for i, timeout := range timeouts {
        if timeout < minTimeout || timeout > maxTimeout+50*time.Millisecond {
            t.Errorf("Timeout %d outside range: %v (expected %v-%v)", i, timeout, minTimeout, maxTimeout)
        }
    }

    // Check that timeouts are not all the same (randomness)
    allSame := true
    first := timeouts[0]
    tolerance := 20 * time.Millisecond
    for _, timeout := range timeouts[1:] {
        if timeout < first-tolerance || timeout > first+tolerance {
            allSame = false
            break
        }
    }
    if allSame {
        t.Error("All timeouts are too similar, randomness might not be working")
    }
}

// Test pause functionality
func TestElectionTicker_Pause(t *testing.T) {
    minTimeout := 50 * time.Millisecond
    maxTimeout := 100 * time.Millisecond
    ticker := NewElectionTicker(minTimeout, maxTimeout)

    ticker.Start()
    defer ticker.Stop()

    // Let it run briefly, then pause
    time.Sleep(10 * time.Millisecond)
    ticker.Pause()

    // Wait longer than max timeout - should not receive timeout while paused
    select {
    case <-ticker.GetTimeoutChan():
        t.Error("Received timeout while ticker was paused")
    case <-time.After(maxTimeout + 50*time.Millisecond):
        // Expected - no timeout should occur when paused
    }
}

// Test reset functionality from running state
func TestElectionTicker_ResetFromRunning(t *testing.T) {
    minTimeout := 100 * time.Millisecond
    maxTimeout := 200 * time.Millisecond
    ticker := NewElectionTicker(minTimeout, maxTimeout)

    ticker.Start()
    defer ticker.Stop()

    // Wait for half the minimum timeout, then reset
    time.Sleep(minTimeout / 2)
    ticker.Reset()

    // Timeout should occur after reset within the timeout range
    start := time.Now()
    select {
    case <-ticker.GetTimeoutChan():
        elapsed := time.Since(start)
        if elapsed < minTimeout || elapsed > maxTimeout+50*time.Millisecond {
            t.Errorf("Timeout after reset outside expected range: %v", elapsed)
        }
    case <-time.After(maxTimeout + 100*time.Millisecond):
        t.Error("No timeout after reset")
    }
}

// Test reset functionality from paused state (this should resume the ticker)
func TestElectionTicker_ResetFromPaused(t *testing.T) {
    minTimeout := 50 * time.Millisecond
    maxTimeout := 100 * time.Millisecond
    ticker := NewElectionTicker(minTimeout, maxTimeout)

    ticker.Start()
    defer ticker.Stop()

    // Pause the ticker
    ticker.Pause()

    // Verify it's paused
    select {
    case <-ticker.GetTimeoutChan():
        t.Error("Received timeout while paused")
    case <-time.After(maxTimeout + 50*time.Millisecond):
        // Expected - paused
    }

    // Reset should resume the ticker and start a new timeout
    ticker.Reset()
    start := time.Now()
    select {
    case <-ticker.GetTimeoutChan():
        elapsed := time.Since(start)
        if elapsed < minTimeout || elapsed > maxTimeout+50*time.Millisecond {
            t.Errorf("Timeout after reset from paused outside expected range: %v", elapsed)
        }
    case <-time.After(maxTimeout + 100*time.Millisecond):
        t.Error("No timeout after reset from paused state")
    }
}

// Test multiple resets
func TestElectionTicker_MultipleResets(t *testing.T) {
    minTimeout := 50 * time.Millisecond
    maxTimeout := 100 * time.Millisecond
    ticker := NewElectionTicker(minTimeout, maxTimeout)

    ticker.Start()
    defer ticker.Stop()

    // Reset multiple times quickly
    for i := 0; i < 5; i++ {
        time.Sleep(10 * time.Millisecond)
        ticker.Reset()
    }

    // Should still get a timeout after the last reset
    start := time.Now()
    select {
    case <-ticker.GetTimeoutChan():
        elapsed := time.Since(start)
        if elapsed < minTimeout || elapsed > maxTimeout+50*time.Millisecond {
            t.Errorf("Timeout after multiple resets outside expected range: %v", elapsed)
        }
    case <-time.After(maxTimeout + 100*time.Millisecond):
        t.Error("No timeout after multiple resets")
    }
}

// Test stop functionality
func TestElectionTicker_Stop(t *testing.T) {
    minTimeout := 50 * time.Millisecond
    maxTimeout := 100 * time.Millisecond
    ticker := NewElectionTicker(minTimeout, maxTimeout)

    ticker.Start()

    // Let it run briefly
    time.Sleep(10 * time.Millisecond)

    // Stop the ticker
    ticker.Stop()

    // Should not receive any more timeouts
    select {
    case <-ticker.GetTimeoutChan():
        t.Error("Received timeout after stop")
    case <-time.After(maxTimeout + 50*time.Millisecond):
        // Expected - no timeout should occur after stop
    }
}

// Test stop from paused state
func TestElectionTicker_StopFromPaused(t *testing.T) {
    minTimeout := 50 * time.Millisecond
    maxTimeout := 100 * time.Millisecond
    ticker := NewElectionTicker(minTimeout, maxTimeout)

    ticker.Start()
    ticker.Pause()

    // Stop while paused
    ticker.Stop()

    // Should not receive any timeouts
    select {
    case <-ticker.GetTimeoutChan():
        t.Error("Received timeout after stop from paused")
    case <-time.After(maxTimeout + 50*time.Millisecond):
        // Expected
    }
}

// Test edge case where min and max timeouts are equal
func TestElectionTicker_EqualMinMax(t *testing.T) {
    timeout := 100 * time.Millisecond
    ticker := NewElectionTicker(timeout, timeout)

    ticker.Start()
    defer ticker.Stop()

    start := time.Now()
    select {
    case <-ticker.GetTimeoutChan():
        elapsed := time.Since(start)
        if elapsed < timeout || elapsed > timeout+50*time.Millisecond {
            t.Errorf("Timeout with equal min/max outside expected range: %v (expected ~%v)", elapsed, timeout)
        }
    case <-time.After(timeout + 100*time.Millisecond):
        t.Error("No timeout with equal min/max")
    }
}

// Test that multiple pause calls are safe
func TestElectionTicker_MultiplePause(t *testing.T) {
    minTimeout := 50 * time.Millisecond
    maxTimeout := 100 * time.Millisecond
    ticker := NewElectionTicker(minTimeout, maxTimeout)

    ticker.Start()
    defer ticker.Stop()

    // Multiple pauses should be safe
    ticker.Pause()
    ticker.Pause()
    ticker.Pause()

    // Should still be paused
    select {
    case <-ticker.GetTimeoutChan():
        t.Error("Received timeout after multiple pauses")
    case <-time.After(maxTimeout + 50*time.Millisecond):
        // Expected
    }
}

// Test operations on stopped ticker should be safe
func TestElectionTicker_OperationsAfterStop(t *testing.T) {
    minTimeout := 50 * time.Millisecond
    maxTimeout := 100 * time.Millisecond
    ticker := NewElectionTicker(minTimeout, maxTimeout)

    ticker.Start()
    ticker.Stop()

    // Operations on stopped ticker should be safe and not panic
    ticker.Pause()
    ticker.Reset() // This should be safe but won't do anything
    ticker.Stop()  // Multiple stops should be safe

    // Should not receive any timeouts
    select {
    case <-ticker.GetTimeoutChan():
        t.Error("Received timeout after stop and operations")
    case <-time.After(maxTimeout + 50*time.Millisecond):
        // Expected
    }
}

// Test channel behavior - buffered channel should not block
func TestElectionTicker_ChannelBehavior(t *testing.T) {
    minTimeout := 20 * time.Millisecond
    maxTimeout := 40 * time.Millisecond
    ticker := NewElectionTicker(minTimeout, maxTimeout)

    ticker.Start()
    defer ticker.Stop()

    // Get first timeout but don't read from channel immediately
    <-ticker.GetTimeoutChan()

    // Let multiple timeouts occur
    time.Sleep(maxTimeout * 3)

    // Should be able to read from channel (at most 1 more timeout due to buffering)
    timeoutCount := 0
    for {
        select {
        case <-ticker.GetTimeoutChan():
            timeoutCount++
        case <-time.After(10 * time.Millisecond):
            // No more timeouts available
            goto done
        }
    }
done:
    // Should have received at least one more timeout
    if timeoutCount == 0 {
        t.Error("Expected at least one more timeout")
    }
    // But not too many due to channel buffering
    if timeoutCount > 2 {
        t.Errorf("Too many timeouts received: %d (channel should be buffered)", timeoutCount)
    }
}

// Test pause before any timeout occurs
func TestElectionTicker_PauseBeforeTimeout(t *testing.T) {
    minTimeout := 200 * time.Millisecond
    maxTimeout := 300 * time.Millisecond
    ticker := NewElectionTicker(minTimeout, maxTimeout)

    ticker.Start()
    defer ticker.Stop()

    // Pause immediately before any timeout can occur
    ticker.Pause()

    // Should not receive timeout
    select {
    case <-ticker.GetTimeoutChan():
        t.Error("Received timeout immediately after pause")
    case <-time.After(maxTimeout + 50*time.Millisecond):
        // Expected
    }
}

// Test reset timing precision
func TestElectionTicker_ResetTiming(t *testing.T) {
    minTimeout := 100 * time.Millisecond
    maxTimeout := 150 * time.Millisecond
    ticker := NewElectionTicker(minTimeout, maxTimeout)

    ticker.Start()
    defer ticker.Stop()

    // Wait almost until timeout, then reset
    time.Sleep(minTimeout - 10*time.Millisecond)
    resetTime := time.Now()
    ticker.Reset()

    // The next timeout should occur after the reset time, not the original start time
    select {
    case <-ticker.GetTimeoutChan():
        elapsed := time.Since(resetTime)
        if elapsed < minTimeout {
            t.Errorf("Timeout occurred too early after reset: %v (expected >= %v)", elapsed, minTimeout)
        }
        if elapsed > maxTimeout+50*time.Millisecond {
            t.Errorf("Timeout occurred too late after reset: %v (expected <= %v)", elapsed, maxTimeout)
        }
    case <-time.After(maxTimeout + 100*time.Millisecond):
        t.Error("No timeout after reset")
    }
}
