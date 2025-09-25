package repo

import (
	"context"

	"github.com/jmoiron/sqlx"
)

type EventRepo interface {
	PreProcess(ctx context.Context, n uint64) ([]PackEvent, error)

	Lock(ctx context.Context, n uint64) ([]PackEvent, error)
	Unlock(ctx context.Context, eventIDs []uint64) error

	Remove(ctx context.Context, eventIDs []uint64) error
}

type eventRepo struct {
	db *sqlx.DB
}

type PackEvent struct {
	ID     uint64  `db:"id"`
	Type   string  `db:"type"`
	Entity []uint8 `db:"payload"`
}

func NewEventRepo(db *sqlx.DB) EventRepo {
	return &eventRepo{db: db}
}
