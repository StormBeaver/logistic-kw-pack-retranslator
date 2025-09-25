package repo

import (
	"context"
	"fmt"

	sq "github.com/Masterminds/squirrel"
	"github.com/opentracing/opentracing-go"
)

func (e eventRepo) PreProcess(ctx context.Context, count uint64) ([]PackEvent, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "repo.PreProcess")
	defer span.Finish()

	err := AcquireLock(ctx, e.db)

	if err != nil {
		return nil, fmt.Errorf("try lock PreProcess: %w", err)
	}

	sQuery := sq.Select("id", "type", "payload").
		From("packs_events").
		Where(sq.Eq{"lock": true}).
		Limit(count).
		RunWith(e.db).
		PlaceholderFormat(sq.Dollar)

	sql, args, err := sQuery.ToSql()
	if err != nil {
		return nil, fmt.Errorf("convert to sql: %w", err)
	}

	events := make([]PackEvent, 0, count)

	err = e.db.SelectContext(ctx, &events, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("exec query in Lock: %w", err)
	}

	return events, Unlock(ctx, e.db)
}
