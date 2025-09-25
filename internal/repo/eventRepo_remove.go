package repo

import (
	"context"
	"fmt"

	sq "github.com/Masterminds/squirrel"
	"github.com/opentracing/opentracing-go"
)

func (e eventRepo) Remove(ctx context.Context, eventIDs []uint64) error {
	span, ctx := opentracing.StartSpanFromContext(ctx, "repo.Remove")
	defer span.Finish()

	Query := sq.Delete("packs_events").
		Where(sq.Eq{"id": eventIDs}).
		RunWith(e.db).
		PlaceholderFormat(sq.Dollar)

	rows, err := Query.QueryContext(ctx)
	if err != nil {
		return fmt.Errorf("remove qurey: %w", err)
	}
	rows.Close()
	return nil
}
