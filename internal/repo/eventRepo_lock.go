package repo

import (
	"context"
	"fmt"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/opentracing/opentracing-go"
	"github.com/rs/zerolog/log"
)

func (e eventRepo) Lock(ctx context.Context, count uint64) ([]PackEvent, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "repo.Lock")
	defer span.Finish()

	events := make([]PackEvent, 0, count)
	ids := make([]uint64, 0, count)

	err := AcquireLock(ctx, e.db)

	if err != nil {
		return nil, fmt.Errorf("try Lock: %w", err)
	}

	// it will be beautifull to make subq isinde sQury, but idk how
	subQ := sq.Select("id").
		From("packs_events").
		Where(sq.Eq{"lock": false}).
		Limit(count).RunWith(e.db).
		PlaceholderFormat(sq.Dollar)

	sql, args, err := subQ.ToSql()
	if err != nil {
		return nil, fmt.Errorf("convert to sql: %w", err)
	}
	log.Debug().Str("create SQL in subQ", sql)

	err = e.db.SelectContext(ctx, &ids, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("exec query in subQ: %w", err)
	}

	sQuery := sq.Update("packs_events").
		Set("lock", true).
		Set("updated", time.Now()).
		Where(sq.Eq{"id": ids}).
		Suffix("RETURNING id, type, payload").
		RunWith(e.db).
		PlaceholderFormat(sq.Dollar)

	sql, args, err = sQuery.ToSql()
	log.Debug().Str("create SQL in mainQ", sql)
	if err != nil {
		return nil, fmt.Errorf("convert to sql: %w", err)
	}

	err = e.db.SelectContext(ctx, &events, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("exec query in Lock: %w", err)
	}

	return events, Unlock(ctx, e.db)
}
