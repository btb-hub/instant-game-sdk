package main

import (
	"encoding/json"
	"sync"

	"github.com/gorilla/websocket"
)

type Hub struct {
	mu    sync.RWMutex
	conns map[*websocket.Conn]struct{}
}

type IHub interface {
	Broadcast(msg any)
}

func (h *Hub) Broadcast(msg any) {
	b, _ := json.Marshal(msg)
	h.mu.RLock()
	defer h.mu.RUnlock()
	for c := range h.conns {
		c.WriteMessage(websocket.TextMessage, b)
	}
}
