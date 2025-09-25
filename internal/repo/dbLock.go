package repo

import (
	"context"

	"github.com/jmoiron/sqlx"
)

// lock for tx
func AcquireLockTx(ctx context.Context, tx *sqlx.Tx) error {
	_, err := tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock(hashtext('txLock'))")
	return err
}

// lock for regular sql requests
func AcquireLock(ctx context.Context, db *sqlx.DB) error {
	_, err := db.ExecContext(ctx, "SELECT pg_advisory_lock(hashtext('dbLock'))")
	return err
}

// unlock for regular sql requests
func Unlock(ctx context.Context, db *sqlx.DB) error {
	_, err := db.ExecContext(ctx, "SELECT pg_advisory_unlock(hashtext('dbLock'))")
	return err
}
