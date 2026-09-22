package reconcile

import (
	"testing"

	"example.com/seam/internal/model"
)

func TestCandidateEviction(t *testing.T) {
	candidates := map[int64]model.Account{
		1: {ID: 1, Owner: "a", BalanceCents: 100},
		2: {ID: 2, Owner: "b", BalanceCents: 200},
		3: {ID: 3, Owner: "c", BalanceCents: 300},
	}

	// An update to id 2 evicts it.
	evict(candidates, model.Change{Op: model.OpUpdate, Account: &model.Account{ID: 2}})
	if _, ok := candidates[2]; ok {
		t.Fatal("expected id 2 to be evicted")
	}
	if len(candidates) != 2 {
		t.Fatalf("expected 2 survivors, got %d", len(candidates))
	}

	// A delete to id 3 evicts it.
	evict(candidates, model.Change{Op: model.OpDelete, Account: &model.Account{ID: 3}})
	if _, ok := candidates[3]; ok {
		t.Fatal("expected id 3 to be evicted")
	}

	// An insert to id 4 does not affect existing candidates.
	evict(candidates, model.Change{Op: model.OpInsert, Account: &model.Account{ID: 4}})
	if len(candidates) != 1 {
		t.Fatalf("expected 1 survivor, got %d", len(candidates))
	}
}

func evict(candidates map[int64]model.Account, change model.Change) {
	if change.Account != nil {
		delete(candidates, change.Account.ID)
	}
}
