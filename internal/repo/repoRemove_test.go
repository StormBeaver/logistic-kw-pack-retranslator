package repo_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v4"
	_ "github.com/jackc/pgx/v4/stdlib"
	_ "github.com/lib/pq"
	"github.com/stormbeaver/logistic-pack-retranslator/internal/config"
	"github.com/stormbeaver/logistic-pack-retranslator/internal/database"
	"github.com/stormbeaver/logistic-pack-retranslator/internal/repo"
)

func TestRemove(t *testing.T) {
	if err := config.ReadConfigYML("config.yml"); err != nil {
		panic(err)
	}

	cfg := config.GetConfigInstance()
	dsn := fmt.Sprintf("host=%v port=%v user=%v password=%v dbname=%v sslmode=%v",
		cfg.Database.Host,
		cfg.Database.Port,
		cfg.Database.User,
		cfg.Database.Password,
		cfg.Database.Name,
		cfg.Database.SslMode,
	)

	ctx := context.Background()
	initCtx, cancel := context.WithTimeout(ctx, 10*time.Second)

	defer cancel()
	db, err := database.NewPostgres(initCtx, dsn, cfg.Database.Driver, &cfg.Database.Connections)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	e := repo.NewEventRepo(db)
	delSlc := []uint64{71, 72, 73, 74, 75, 76, 77, 78, 79, 70}
	e.Remove(ctx, delSlc)

}
