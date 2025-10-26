package main

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/gorilla/websocket"
	"github.com/quay/zlog"
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
		err := c.WriteMessage(websocket.TextMessage, b)
		if err != nil {
			zlog.Error(context.Background()).Err(err).Msgf(
				"Error writing message to websocket: %s",
				c.RemoteAddr().String(),
			)
			delete(h.conns, c)
			zlog.Info(context.Background()).Msg(
				"Websocket connection closed",
			)
		}
	}
}
