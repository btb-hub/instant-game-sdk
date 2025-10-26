package main

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
)

func TestScheduleRound_FullLifecycle(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// minimal hub
	h := &Hub{}

	// short timings; reveal and finish will be scheduled relative to ServerStartAt (now+250ms)
	rm := NewRaceManager(h, 10*time.Millisecond, 20*time.Millisecond)

	r := &Round{
		ID:             "r-test",
		Phase:          Betting,
		BettingCloseAt: time.Now().Add(-50 * time.Millisecond), // already closed
	}

	// run scheduler
	go rm.ScheduleRound(ctx, r)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		r.mu.RLock()
		phase := r.Phase
		r.mu.RUnlock()
		if phase == Settled {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Read the final state with proper locking
	r.mu.RLock()
	finalPhase := r.Phase
	finalSeed := r.Seed
	finalWinner := r.Winner
	finalSeq := atomic.LoadUint64(&r.Seq)
	r.mu.RUnlock()

	if finalPhase != Settled {
		t.Fatalf("round did not settle in time; phase=%s seq=%d", finalPhase, finalSeq)
	}
	if finalSeed == nil {
		t.Fatalf("seed not revealed")
	}
	if finalWinner == nil {
		t.Fatalf("winner not set")
	}
	if finalSeq < 5 {
		t.Fatalf("seq increments too small: %d", finalSeq)
	}
}

func TestScheduleRound_BroadcastCallbacks(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Use mockery-generated mock
	mockHub := NewMockIHub(t)

	// Capture all broadcasts
	var broadcasts [][]byte
	mockHub.On("Broadcast", mock.Anything).Run(
		func(args mock.Arguments) {
			msg := args.Get(0)
			if msgBytes, ok := msg.(*[]byte); ok {
				bytes := make([]byte, len(*msgBytes))
				copy(bytes, *msgBytes)
				broadcasts = append(broadcasts, bytes)
			}
		},
	).Return()

	// short timings
	rm := NewRaceManager(mockHub, 10*time.Millisecond, 20*time.Millisecond)

	r := &Round{
		ID:             "r-broadcast-test",
		Phase:          Betting,
		BettingCloseAt: time.Now().Add(-50 * time.Millisecond), // already closed
	}

	// run scheduler
	go rm.ScheduleRound(ctx, r)

	// wait for settlement
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		r.mu.RLock()
		phase := r.Phase
		r.mu.RUnlock()
		if phase == Settled {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	// give a bit of time for final broadcast
	time.Sleep(50 * time.Millisecond)

	// verify we got broadcasts for all phases
	expectedPhases := []Phase{Betting, Locked, Started, Revealed, Finished, Settled}
	if len(broadcasts) != len(expectedPhases) {
		t.Fatalf("expected %d broadcasts, got %d", len(expectedPhases), len(broadcasts))
	}

	// verify phase order and content
	for i, expectedPhase := range expectedPhases {
		var msg Message[any]
		err := json.Unmarshal(broadcasts[i], &msg)
		if err != nil {
			t.Fatalf("failed to unmarshal broadcast[%d]: %v", i, err)
		}

		if msg.Type != expectedPhase {
			t.Errorf("broadcast[%d]: expected phase %s, got %s", i, expectedPhase, msg.Type)
		}

		// Verify data is present
		if msg.Data == nil {
			t.Errorf("broadcast[%d]: missing data", i)
			continue
		}

		// For detailed validation, we need to work with the data as a map
		dataMap, ok := msg.Data.(map[string]any)
		if !ok {
			t.Errorf("broadcast[%d]: data is not a map", i)
			continue
		}

		// verify Locked broadcast has timing information
		if expectedPhase == Locked {
			if _, ok := dataMap["server_start_at"]; !ok {
				t.Error("Locked broadcast missing server_start_at")
			}
			if _, ok := dataMap["reveal_at"]; !ok {
				t.Error("Locked broadcast missing reveal_at")
			}
			if _, ok := dataMap["finish_at"]; !ok {
				t.Error("Locked broadcast missing finish_at")
			}
		}

		// verify Revealed broadcast has seed and winner
		if expectedPhase == Revealed {
			if _, ok := dataMap["seed"]; !ok {
				t.Error("Revealed broadcast missing seed")
			}
			if _, ok := dataMap["winner"]; !ok {
				t.Error("Revealed broadcast missing winner")
			}
		}

		// verify seq is present and incrementing
		if i > 0 {
			var prevMsg Message[any]
			_ = json.Unmarshal(broadcasts[i-1], &prevMsg)
			if prevMsg.Data != nil {
				prevDataMap, _ := prevMsg.Data.(map[string]any)
				currentSeq, _ := dataMap["seq"].(float64)
				prevSeq, _ := prevDataMap["seq"].(float64)
				if currentSeq <= prevSeq {
					t.Errorf(
						"seq not incrementing: broadcast[%d].Seq=%.0f, broadcast[%d].Seq=%.0f",
						i-1, prevSeq, i, currentSeq,
					)
				}
			}
		}
	}
}

func TestScheduleRound_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	mockHub := NewMockIHub(t)
	mockHub.On("Broadcast", mock.Anything).Return()

	rm := NewRaceManager(mockHub, 100*time.Millisecond, 200*time.Millisecond)

	r := &Round{
		ID:             "r-cancel-test",
		Phase:          Betting,
		BettingCloseAt: time.Now().Add(50 * time.Millisecond),
	}

	done := make(chan struct{})
	go func() {
		rm.ScheduleRound(ctx, r)
		close(done)
	}()

	// cancel context shortly after starting
	time.Sleep(30 * time.Millisecond)
	cancel()

	// verify scheduler exits promptly
	select {
	case <-done:
		// success
	case <-time.After(500 * time.Millisecond):
		t.Fatal("ScheduleRound did not exit after context cancellation")
	}

	// verify round did not reach Settled
	r.mu.RLock()
	finalPhase := r.Phase
	r.mu.RUnlock()

	if finalPhase == Settled {
		t.Error("round should not have settled after context cancellation")
	}
}
