package mysql_test

import (
	"database/sql"
	"os"
	"testing"

	"github.com/Ali127Dev/xoutbox/internal/sqlstore"
	"github.com/Ali127Dev/xoutbox/internal/storetest"
	"github.com/Ali127Dev/xoutbox/mysql"
	_ "github.com/go-sql-driver/mysql"
)

// XOUTBOX_MYSQL_DSN=root:root@tcp(localhost:3306)/outbox
func TestStore(t *testing.T) {
	dsn := os.Getenv("XOUTBOX_MYSQL_DSN")
	if dsn == "" {
		t.Skip("XOUTBOX_MYSQL_DSN not set")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	storetest.Run(t, db, func(o ...sqlstore.Option) storetest.Store { return mysql.New(db, o...) })
}
