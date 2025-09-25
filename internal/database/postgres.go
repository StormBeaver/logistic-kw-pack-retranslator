package database

import (
	"fmt"

	sq "github.com/Masterminds/squirrel"
	"github.com/rs/zerolog/log"
	"github.com/stormbeaver/logistic-pack-retranslator/internal/config"
	"golang.org/x/net/context"

	"github.com/jmoiron/sqlx"
)

// placeholder for sql requests
var StatementBuilder = sq.StatementBuilder.PlaceholderFormat(sq.Dollar)

// NewPostgres returns DB
func NewPostgres(ctx context.Context, dsn, driver string, dbCons *config.DBCons) (*sqlx.DB, error) {
	db, err := sqlx.Open(driver, dsn)
	if err != nil {
		log.Error().Err(err).Msgf("failed to create database connection")
		return nil, fmt.Errorf("%w, sqlx.Open(%s, %s)", err, driver, dsn)
	}

	db.SetMaxOpenConns(dbCons.MaxOpenCons)
	db.SetMaxIdleConns(dbCons.MaxIdleCons)
	db.SetConnMaxLifetime(dbCons.ConnMaxLifeTime)
	db.SetConnMaxIdleTime(dbCons.ConnMaxIdleTime)

	if err = db.PingContext(ctx); err != nil {
		log.Error().Err(err).Msgf("failed ping the database")

		return nil, fmt.Errorf("%w, db.PingContext(%v)", err, ctx)
	}

	return db, nil
}
