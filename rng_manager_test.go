package instantgame

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"testing"
)

func TestNewRNGManager_SeedFor_And_CommitFor_WithEnvSecret(t *testing.T) {
	// Ensure env-based secret yields deterministic, known values
	const secret = "test-secret"
	const roundID = "round-42"
	_ = os.Setenv("RNG_SECRET", secret)
	t.Cleanup(func() { _ = os.Unsetenv("RNG_SECRET") })

	m := NewRNGManager()

	// Expected seed = HMAC-SHA256(secret, "seed:"+roundID)
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte("seed:"))
	mac.Write([]byte(roundID))
	expectedSeed := hex.EncodeToString(mac.Sum(nil))

	seed := m.SeedFor(roundID)
	if seed != expectedSeed {
		t.Fatalf("SeedFor mismatch. got=%s want=%s", seed, expectedSeed)
	}

	// Commit is SHA-256 of seed bytes (hex decoded)
	seedBytes, err := hex.DecodeString(expectedSeed)
	if err != nil {
		t.Fatalf("hex decode seed: %v", err)
	}
	sum := sha256.Sum256(seedBytes)
	expectedCommit := hex.EncodeToString(sum[:])

	commit := m.CommitFor(roundID)
	if commit != expectedCommit {
		t.Fatalf("CommitFor mismatch. got=%s want=%s", commit, expectedCommit)
	}
}

func TestNewRNGManager_NoEnvSecret_ProducesHexSeed(t *testing.T) {
	_ = os.Unsetenv("RNG_SECRET")
	m := NewRNGManager()
	seed := m.SeedFor("any-round")
	if len(seed) != 64 {
		t.Fatalf("seed length = %d, want 64 hex chars", len(seed))
	}
	if _, err := hex.DecodeString(seed); err != nil {
		t.Fatalf("seed is not valid hex: %v", err)
	}
}

func TestPickWinnerIndex_RangeAndDeterminism(t *testing.T) {
	_ = os.Setenv("RNG_SECRET", "deterministic-key")
	t.Cleanup(func() { _ = os.Unsetenv("RNG_SECRET") })
	m := NewRNGManager()
	r := &Round{ID: "r42"}
	seed := m.SeedFor(r.ID)

	// n == 2 only allows index 2
	idx, err := m.PickWinnerIndex(seed, r, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if idx != 2 {
		t.Fatalf("for n=2, winner must be 2, got %d", idx)
	}

	// a larger n within [2..n] and deterministic across calls
	n := 10
	idx1, err := m.PickWinnerIndex(seed, r, n)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if idx1 < 2 || idx1 > n {
		t.Fatalf("index out of range: got %d, want [2..%d]", idx1, n)
	}
	idx2, err := m.PickWinnerIndex(seed, r, n)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if idx1 != idx2 {
		t.Fatalf("non-deterministic result: %d vs %d", idx1, idx2)
	}
}

func TestPickWinnerIndex_ErrorForSmallN(t *testing.T) {
	m := NewRNGManager()
	r := &Round{ID: "r1"}
	if _, err := m.PickWinnerIndex("seed", r, 1); err == nil {
		t.Fatalf("expected error for n<2")
	}
}
