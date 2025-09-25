package repo

import (
	"context"
	"fmt"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/opentracing/opentracing-go"
)

func (e eventRepo) Unlock(ctx context.Context, eventIDs []uint64) error {
	span, ctx := opentracing.StartSpanFromContext(ctx, "repo.Unlock")
	defer span.Finish()

	Query := sq.Update("packs_events").
		Set("lock", false).
		Set("updated", time.Now()).
		Where(sq.Eq{"id": eventIDs}).
		RunWith(e.db).
		PlaceholderFormat(sq.Dollar)

	rows, err := Query.QueryContext(ctx)
	if err != nil {
		return fmt.Errorf("update qurey: %w", err)
	}
	rows.Close()
	return nil
}
