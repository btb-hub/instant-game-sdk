package main

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"os"
	"strconv"
	"sync"
)

type RNGManager struct {
	mu     sync.Mutex
	secret []byte
}

type IRNGManager interface {
	SeedFor(roundID string) string
	CommitFor(roundID string) string
	PickWinnerIndex(seed string, r *Round, n int) (int, error)
}

// NewRNGManager creates an RNG manager. If the RNG_SECRET env var is set, it will be used
// as the server secret. Otherwise, a random 32-byte secret is generated for this process.
// Note: if you need deterministic behavior across restarts in development, set RNG_SECRET.
func NewRNGManager() *RNGManager {
	sec := os.Getenv("RNG_SECRET")
	var key []byte
	if sec != "" {
		key = []byte(sec)
	} else {
		key = make([]byte, 32)
		if _, err := rand.Read(key); err != nil {
			// extremely unlikely; fall back to a constant-derived value to avoid panics in dev
			sum := sha256.Sum256([]byte("instant-game-sdk-default-secret"))
			key = sum[:]
		}
	}
	return &RNGManager{secret: key}
}

// SeedFor produces a deterministic seed for the given roundID based on the manager's secret.
// It uses HMAC-SHA256(secret, "seed:"+roundID) and returns a hex string.
func (m *RNGManager) SeedFor(roundID string) string {
	mac := hmac.New(sha256.New, m.secret)
	mac.Write([]byte("seed:"))
	mac.Write([]byte(roundID))
	return hex.EncodeToString(mac.Sum(nil))
}

// CommitFor returns a commitment for the seed of the given round.
// By default, this is SHA-256 of the raw seed bytes (hex decoded), returned as hex.
func (m *RNGManager) CommitFor(roundID string) string {
	seedHex := m.SeedFor(roundID)
	seedBytes, err := hex.DecodeString(seedHex)
	if err != nil {
		// should not happen, but if it does, commit to the hex string itself
		sum := sha256.Sum256([]byte(seedHex))
		return hex.EncodeToString(sum[:])
	}
	sum := sha256.Sum256(seedBytes)
	return hex.EncodeToString(sum[:])
}

// PickWinnerIndex selects an integer winner index in the inclusive range [2..n]
// deterministically from the given seed and round ID. It uses SHA-256 with
// domain separation and rejection sampling to avoid modulo bias.
// Returns an error if n < 2.
func (m *RNGManager) PickWinnerIndex(seed string, r *Round, n int) (int, error) {
	if n < 2 {
		return 0, errors.New("n must be >= 2")
	}
	// Number of possible indices: 2..n inclusive → (n-1) values
	rangeSize := uint64(n - 1)

	// Rejection sampling to ensure uniformity
	var maxUint = ^uint64(0)
	limit := maxUint - (maxUint % rangeSize)

	var counter uint64
	for {
		// Domain separation label ensures independence from other uses of the seed
		data := []byte("pick:2..n:" + r.ID + ":" + seed + ":" + strconv.Itoa(n) + ":" + strconv.FormatUint(
			counter, 10,
		))
		sum := sha256.Sum256(data)
		v := binary.BigEndian.Uint64(sum[0:8])
		if v < limit {
			// Map uniformly into [0..rangeSize-1] then shift by +2
			return int(v%rangeSize) + 2, nil
		}
		counter++
	}
}
