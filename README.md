# Instant Game SDK

A Go library for managing race/betting rounds with provably fair random number generation.

## Features

- **Round Lifecycle Management**: Automated state machine for betting rounds (Betting → Locked → Started → Revealed → Finished → Settled)
- **Provably Fair RNG**: Commit-reveal scheme for transparent random number generation
- **Thread-Safe**: Built-in mutex protection for concurrent access
- **WebSocket Broadcasting**: Real-time updates to connected clients
- **Configurable Timings**: Customizable durations for betting windows, reveals, and round completion

## Installation

```bash
go get github.com/yourusername/instant-game-sdk
```

## Quick Start

```go
package main

import (
    "context"
    "time"
)

func main() {
    // Create a hub for broadcasting messages to clients
    hub := &Hub{}

    // Create race manager with custom timings
    // revealTime: 5s from start to reveal winner
    // duration: 10s total round duration
    rm := NewRaceManager(hub, 5*time.Second, 10*time.Second)

    // Generate a new round
    round := rm.GenerateRound()

    // Start the round scheduler
    ctx := context.Background()
    rm.ScheduleRound(ctx, round)
}
```

## Round Lifecycle

Each round goes through 6 phases:

1. **BETTING** - Users can place bets
2. **LOCKED** - Betting closed, animation about to start
3. **STARTED** - Race animation running
4. **REVEALED** - Winner revealed with seed
5. **FINISHED** - Animation complete
6. **SETTLED** - Payouts processed

## Configuration

### Basic Configuration

```go
rm := NewRaceManager(hub, revealTime, duration)
```

- `hub`: Broadcast hub for WebSocket connections
- `revealTime`: Duration from STARTED to REVEALED phase
- `duration`: Total duration from STARTED to FINISHED phase

### Advanced Configuration

```go
rm := NewRaceManagerWithConfig(
    hub,
    rng,
    5*time.Second,  // revealTime
    10*time.Second, // duration
    30*time.Second, // bettingDuration
    250*time.Millisecond, // startDelay
)
```

- `rng`: Custom RNG manager (implements `IRNGManager`)
- `bettingDuration`: How long users have to place bets (default: 30s)
- `startDelay`: Delay between LOCKED and STARTED (default: 250ms)

## API Reference

### RoundManager

#### `GenerateRound() *Round`

Creates a new round in the BETTING phase with a unique ID.

```go
round := rm.GenerateRound()
fmt.Println(round.ID) // UUID string
fmt.Println(round.Phase) // "BETTING"
```

#### `ScheduleRound(ctx context.Context, r *Round)`

Manages the round state machine lifecycle. Broadcasts updates to all connected clients on phase transitions.

```go
ctx, cancel := context.WithCancel(context.Background())
defer cancel()

go rm.ScheduleRound(ctx, round)
```

### Round Structure

```go
type Round struct {
    ID             string     // Unique round identifier
    Phase          Phase      // Current phase
    Pot            int64      // Total pot amount
    Commit         *string    // RNG commit (before reveal)
    Seed           *string    // RNG seed (after reveal)
    Winner         *int       // Winner index (0-9)
    BettingCloseAt time.Time  // When betting closes
    ServerStartAt  *time.Time // When animation starts
    RevealAt       *time.Time // When winner is revealed
    FinishAt       *time.Time // When round finishes
    Seq            uint64     // Sequence number for ordering
}
```

### Broadcast Messages

All phase transitions broadcast messages in this format:

```json
{
    "type": "LOCKED",
    "time": 1234567890,
    "data": {
        "id": "550e8400-e29b-41d4-a716-446655440000",
        "phase": "LOCKED",
        "pot": 1000,
        "server_start_at": "2024-01-01T12:00:00Z",
        "reveal_at": "2024-01-01T12:00:05Z",
        "finish_at": "2024-01-01T12:00:10Z",
        "seq": 1
    }
}
```

## Provably Fair RNG

The library uses a commit-reveal scheme for transparent random number generation:

1. **Commit Phase**: Server generates and commits to a seed before bets are placed
2. **Reveal Phase**: After betting closes, seed is revealed
3. **Verification**: Anyone can verify the winner was determined fairly using the revealed seed

```go
rng := NewRNGManager()

// Generate commit for a round
commit := rng.CommitFor("round-id")

// Later, reveal the seed
seed := rng.SeedFor("round-id")

// Pick winner (0-9)
winner, err := rng.PickWinnerIndex(seed, round, 10)
```

## Thread Safety

The library is thread-safe:

- Round fields are protected by `sync.RWMutex`
- Sequence numbers use atomic operations
- Safe for concurrent access from multiple goroutines

```go
// Safe to read from multiple goroutines
round.mu.RLock()
phase := round.Phase
round.mu.RUnlock()

// Atomic sequence access
seq := atomic.LoadUint64(&round.Seq)
```

## Examples

### Custom Hub Implementation

```go
type CustomHub struct {
    clients map[*websocket.Conn]bool
}

func (h *CustomHub) Broadcast(msg any) {
    // Custom broadcast logic
    for client := range h.clients {
        client.WriteJSON(msg)
    }
}
```

### Graceful Shutdown

```go
ctx, cancel := context.WithCancel(context.Background())

go rm.ScheduleRound(ctx, round)

// Later, cancel to stop the scheduler
cancel()
```

### Multiple Concurrent Rounds

```go
hub := &Hub{}
rm := NewRaceManager(hub, 5*time.Second, 10*time.Second)

for i := 0; i < 5; i++ {
    round := rm.GenerateRound()
    go rm.ScheduleRound(context.Background(), round)
}
```

## Testing

Run tests:

```bash
go test -v
```

The library includes comprehensive tests for:
- Full round lifecycle
- Broadcast callbacks
- Context cancellation
- Thread safety
- RNG fairness

## License

MIT License
