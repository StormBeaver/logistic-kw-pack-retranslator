package repo

import (
	"context"

	"github.com/jmoiron/sqlx"
)

// lock for regular sql requests
func AcquireLock(ctx context.Context, db *sqlx.DB) error {
	_, err := db.ExecContext(ctx, "SELECT pg_advisory_lock(523698741)")
	return err
}

// unlock for regular sql requests
func Unlock(ctx context.Context, db *sqlx.DB) error {
	_, err := db.ExecContext(ctx, "SELECT pg_advisory_unlock(523698741)")
	return err
}
