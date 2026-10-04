package postgres_test

import (
	"database/sql"
	"os"
	"testing"

	"github.com/Ali127Dev/xoutbox/internal/sqlstore"
	"github.com/Ali127Dev/xoutbox/internal/storetest"
	"github.com/Ali127Dev/xoutbox/postgres"
	_ "github.com/jackc/pgx/v5/stdlib"
)

// XOUTBOX_POSTGRES_DSN=postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable
func TestStore(t *testing.T) {
	dsn := os.Getenv("XOUTBOX_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("XOUTBOX_POSTGRES_DSN not set")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	storetest.Run(t, db, func(o ...sqlstore.Option) storetest.Store { return postgres.New(db, o...) })
}
