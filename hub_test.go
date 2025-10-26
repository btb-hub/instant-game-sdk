package main

import (
	"testing"
)

func TestHubBroadcast_NoConns_NoPanic(t *testing.T) {
	h := &Hub{}
	// Ensure a method can be called with no connections and arbitrary message
	h.Broadcast(map[string]any{"hello": "world", "n": 42})
}

func TestHub_GetRoundCh(t *testing.T) {
	ch := make(chan *Round, 10)
	h := &Hub{
		roundUpdated: ch,
	}

	// Verify GetRoundCh returns the correct channel
	returned := h.GetRoundCh()
	if returned != ch {
		t.Fatal("GetRoundCh returned different channel")
	}

	// Verify same channel on multiple calls
	returned2 := h.GetRoundCh()
	if returned != returned2 {
		t.Error("GetRoundCh returned different channel on second call")
	}
}

func TestHub_UpdateRound(t *testing.T) {
	ch := make(chan *Round, 10)
	h := &Hub{
		roundUpdated: ch,
	}

	round := &Round{
		ID:    "test-round",
		Phase: Betting,
	}

	// Send round update
	go h.UpdateRound(round)

	// Verify round is sent to channel
	received := <-ch
	if received.ID != round.ID {
		t.Errorf("expected round ID %s, got %s", round.ID, received.ID)
	}
	if received.Phase != round.Phase {
		t.Errorf("expected phase %s, got %s", round.Phase, received.Phase)
	}
}

func TestHub_UpdateRound_Multiple(t *testing.T) {
	ch := make(chan *Round, 10)
	h := &Hub{
		roundUpdated: ch,
	}

	rounds := []*Round{
		{ID: "round-1", Phase: Betting},
		{ID: "round-2", Phase: Locked},
		{ID: "round-3", Phase: Started},
	}

	// Send multiple updates
	for _, r := range rounds {
		go h.UpdateRound(r)
	}

	// Verify all rounds are received
	receivedIDs := make(map[string]bool)
	for i := 0; i < 3; i++ {
		received := <-ch
		receivedIDs[received.ID] = true
	}

	if len(receivedIDs) != 3 {
		t.Errorf("expected 3 unique rounds, got %d", len(receivedIDs))
	}
}
