// Package sink applies row changes to the destination PostgreSQL accounts table.
package sink

import (
	"context"
	"fmt"

	"example.com/seam/internal/model"
	"github.com/jackc/pgx/v5"
)

// Mutator applies a model.Change to the destination accounts table inside a
// single transaction. It does not commit.
type Mutator struct{}

func NewMutator() *Mutator {
	return &Mutator{}
}

func (m *Mutator) Apply(ctx context.Context, tx pgx.Tx, change model.Change) error {
	if change.Account == nil {
		return fmt.Errorf("change has no account payload")
	}
	switch change.Op {
	case model.OpInsert, model.OpUpdate:
		return m.upsert(ctx, tx, *change.Account)
	case model.OpDelete:
		return m.delete(ctx, tx, change.Account.ID)
	default:
		return fmt.Errorf("unsupported operation %q", change.Op)
	}
}

func (m *Mutator) upsert(ctx context.Context, tx pgx.Tx, account model.Account) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO accounts (id, owner, balance_cents) VALUES ($1, $2, $3)
		ON CONFLICT (id) DO UPDATE SET owner = EXCLUDED.owner, balance_cents = EXCLUDED.balance_cents`,
		account.ID, account.Owner, account.BalanceCents)
	if err != nil {
		return fmt.Errorf("upsert id=%d: %w", account.ID, err)
	}
	return nil
}

func (m *Mutator) delete(ctx context.Context, tx pgx.Tx, id int64) error {
	_, err := tx.Exec(ctx, `DELETE FROM accounts WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("delete id=%d: %w", id, err)
	}
	return nil
}

// WriteCandidates inserts surviving snapshot candidates in one batch.
func (m *Mutator) WriteCandidates(ctx context.Context, tx pgx.Tx, candidates []model.Account) error {
	if len(candidates) == 0 {
		return nil
	}
	// Batch insert with ON CONFLICT. This overwrites any pre-existing row, but
	// every candidate in this window has been proven safe: no CDC touched it
	// between LOW and HIGH.
	for _, account := range candidates {
		if err := m.upsert(ctx, tx, account); err != nil {
			return err
		}
	}
	return nil
}
