package instantgame

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/gorilla/websocket"
	"github.com/quay/zlog"
)

type Hub struct {
	mu           sync.RWMutex
	conns        map[*websocket.Conn]struct{}
	roundUpdated chan *Round
}

type IHub interface {
	Broadcast(msg any)
	GetRoundCh() chan *Round
	UpdateRound(r *Round)
	Register(c *websocket.Conn)
	Unregister(c *websocket.Conn)
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

func (h *Hub) GetRoundCh() chan *Round {
	return h.roundUpdated
}

func (h *Hub) UpdateRound(r *Round) {
	h.roundUpdated <- r
}

func (h *Hub) Register(c *websocket.Conn) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.conns[c] = struct{}{}
}

func (h *Hub) Unregister(c *websocket.Conn) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.conns, c)
}
