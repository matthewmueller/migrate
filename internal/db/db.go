package db

import (
	"database/sql"
	"fmt"

	"github.com/xo/dburl"
)

func Dial(connString string) (*sql.DB, error) {
	url, err := dburl.Parse(connString)
	if err != nil {
		return nil, err
	}
	switch url.Scheme {
	case "postgres", "postgresql":
		return sql.Open("pgx", url.DSN)
	case "sqlite", "sqlite3":
		return sql.Open("sqlite3", url.DSN)
	default:
		return nil, fmt.Errorf("migrate doesn't support this url scheme: %s", url.Scheme)
	}
}
