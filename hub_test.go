package main

import "testing"

func TestHubBroadcast_NoConns_NoPanic(t *testing.T) {
	h := &Hub{}
	// Ensure method can be called with no connections and arbitrary message
	h.Broadcast(map[string]any{"hello": "world", "n": 42})
}
