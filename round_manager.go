package instantgame

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/quay/zlog"
)

type Phase string

type Round struct {
	mu             sync.RWMutex
	ID             string     `json:"id"`
	Phase          Phase      `json:"phase"`
	Pot            int64      `json:"pot"`
	Commit         *string    `json:"commit"`
	Seed           *string    `json:"seed"`
	Winner         *int       `json:"winner"`
	BettingCloseAt time.Time  `json:"betting_close_at"`
	ServerStartAt  *time.Time `json:"server_start_at"`
	RevealAt       *time.Time `json:"reveal_at"`
	FinishAt       *time.Time `json:"finish_at"`
	Seq            uint64     `json:"seq"`
}

const (
	Betting  Phase = "BETTING"
	Locked   Phase = "LOCKED"
	Started  Phase = "STARTED"
	Revealed Phase = "REVEALED"
	Finished Phase = "FINISHED"
	Settled  Phase = "SETTLED"
)

type RoundManager struct {
	hub             IHub
	rng             IRNGManager
	revealTime      time.Duration // time from start to reveal (step 4)
	duration        time.Duration // total duration from start to finish
	bettingDuration time.Duration // betting window (step 2)
	startDelay      time.Duration // delay before starting animation (between locked and started)
	runnersAmount   int
}

type Message[T any] struct {
	Type Phase `json:"type"`
	Time int64 `json:"time"`
	Data T     `json:"data"`
}

type IRoundManager interface {
	ScheduleRound(ctx context.Context, r *Round) *time.Time
	GenerateRound() *Round
}

// NewRoundManager creates a new RoundManager with configurable timings
// revealTime: duration from STARTED to REVEALED (step 4)
// duration: total duration from STARTED to FINISHED
// bettingDuration: how long users have to place bets (step 2, default 30s if 0)
func NewRoundManager(hub IHub, revealTime, duration time.Duration) IRoundManager {
	return NewRoundManagerWithConfig(hub, NewRNGManager(), revealTime, duration, 30*time.Second, 250*time.Millisecond)
}

// NewRoundManagerWithConfig creates a RoundManager with full configuration
func NewRoundManagerWithConfig(
	hub IHub, rng IRNGManager, revealTime, duration, bettingDuration, startDelay time.Duration,
) IRoundManager {
	if bettingDuration == 0 {
		bettingDuration = 30 * time.Second
	}
	if startDelay == 0 {
		startDelay = 250 * time.Millisecond
	}
	return &RoundManager{
		hub:             hub,
		rng:             rng,
		revealTime:      revealTime,
		duration:        duration,
		bettingDuration: bettingDuration,
		startDelay:      startDelay,
		runnersAmount:   10,
	}
}

// GenerateRound creates a new round in the BETTING phase
func (rm *RoundManager) GenerateRound() *Round {
	return &Round{
		ID:             uuid.New().String(),
		Phase:          Betting,
		Pot:            0,
		BettingCloseAt: time.Now().Add(rm.bettingDuration),
		Seq:            0,
	}
}

// ScheduleRound manages the round state machine lifecycle
func (rm *RoundManager) ScheduleRound(
	ctx context.Context, r *Round,
) *time.Time {
	// Step 1: Announce the round creation if in the BETTING phase
	r.mu.RLock()
	currentPhase := r.Phase
	r.mu.RUnlock()

	if currentPhase == Betting {
		m, err := rm.makeMessage(Betting, r)
		if err == nil {
			rm.hub.Broadcast(m)
		}
	}

	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()

	for {
		select {
		case <-ctx.Done():
			n := time.Now()
			return &n
		case now := <-tick.C:
			r.mu.Lock()
			switch r.Phase {
			case Betting:
				// Step 2: Lock betting after betting duration
				if now.After(r.BettingCloseAt) {
					r.Phase = Locked
					atomic.AddUint64(&r.Seq, 1)
					// Calculate timing milestones
					startAt := now.Add(rm.startDelay)
					r.ServerStartAt = &startAt
					revealAt := startAt.Add(rm.revealTime)
					r.RevealAt = &revealAt
					finishAt := startAt.Add(rm.duration)
					r.FinishAt = &finishAt
					r.mu.Unlock()
					m, err := rm.makeMessage(Locked, r)
					if err == nil {
						// Notify clients that betting is locked
						rm.hub.Broadcast(m)
						rm.hub.UpdateRound(r)
					} else {
						zlog.Error(context.Background()).Err(err).Msg(
							"Error broadcasting round locked message",
						)
					}
					continue
				}

			case Locked:
				// Step 3: Start the round animation
				if r.ServerStartAt != nil && now.After(*r.ServerStartAt) {
					r.Phase = Started
					atomic.AddUint64(&r.Seq, 1)
					r.mu.Unlock()
					m, err := rm.makeMessage(Started, r)
					if err == nil {
						rm.hub.Broadcast(m)
						rm.hub.UpdateRound(r)
					} else {
						zlog.Error(context.Background()).Err(err).Msg(
							"Error broadcasting round start message",
						)
					}
					continue
				}

			case Started:
				// Step 4: Reveal the winner after reveal time
				if r.RevealAt != nil && now.After(*r.RevealAt) && r.Seed == nil {
					seed := rm.rng.SeedFor(r.ID)
					r.Seed = &seed
					r.mu.Unlock()
					winner, err := rm.rng.PickWinnerIndex(seed, r, rm.runnersAmount)
					r.mu.Lock()
					if err == nil {
						w := winner
						r.Winner = &w
						r.Phase = Revealed
						atomic.AddUint64(&r.Seq, 1)
						r.mu.Unlock()
						m, err := rm.makeMessage(Revealed, r)
						if err == nil {
							rm.hub.Broadcast(m)
							rm.hub.UpdateRound(r)
						} else {
							zlog.Error(context.Background()).Err(err).Msg(
								"Error broadcasting round reveal message",
							)
						}
						continue
					}
					// Error recovery: retry on the next tick
				}

			case Revealed:
				// Step 5: Finish the round
				if r.FinishAt != nil && now.After(*r.FinishAt) {
					r.Phase = Revealed
					atomic.AddUint64(&r.Seq, 1)
					r.mu.Unlock()
					m, err := rm.makeMessage(Finished, r)
					if err == nil {
						rm.hub.Broadcast(m)
						rm.hub.UpdateRound(r)
					} else {
						zlog.Error(context.Background()).Err(err).Msg(
							"Error broadcasting round finish message",
						)
					}
					continue
				}

			case Finished:
				// Step 6: Settle and perform payroll
				r.Phase = Settled
				atomic.AddUint64(&r.Seq, 1)
				r.mu.Unlock()
				m, err := rm.makeMessage(Settled, r)
				if err == nil {
					rm.hub.Broadcast(m)
				} else {
					zlog.Error(context.Background()).Err(err).Msg(
						"Error broadcasting round settled message",
					)
					continue
				}
				rm.hub.UpdateRound(r)
				n := time.Now()
				return &n // Round complete, exit scheduler
			}
			r.mu.Unlock()
		}
	}
}

// makeMessage creates a JSON-encoded message with the given type and data
func (rm *RoundManager) makeMessage(messageType Phase, data any) (*[]byte, error) {
	m := Message[any]{
		Type: messageType,
		Data: data,
		Time: time.Now().Unix(),
	}

	b, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}

	return &b, nil
}
