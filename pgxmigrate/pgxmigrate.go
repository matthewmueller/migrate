// Package pgxmigrate adapts pgx connection pools for use with migrate.
package pgxmigrate

import (
	"io/fs"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/matthewmueller/migrate"
)

// RemoteVersion fetches the latest remote version.
func RemoteVersion(pool *pgxpool.Pool, fsys fs.FS, tableName string) (name string, err error) {
	db := stdlib.OpenDBFromPool(pool)
	defer db.Close()
	return migrate.RemoteVersion(db, fsys, tableName)
}

// Up migrates the database up to the latest migration.
func Up(log *slog.Logger, pool *pgxpool.Pool, fsys fs.FS, tableName string) error {
	db := stdlib.OpenDBFromPool(pool)
	defer db.Close()
	return migrate.Up(log, db, fsys, tableName)
}

// UpBy migrates the database up by i.
func UpBy(log *slog.Logger, pool *pgxpool.Pool, fsys fs.FS, tableName string, i int) error {
	db := stdlib.OpenDBFromPool(pool)
	defer db.Close()
	return migrate.UpBy(log, db, fsys, tableName, i)
}

// Down migrates the database down to 0.
func Down(log *slog.Logger, pool *pgxpool.Pool, fsys fs.FS, tableName string) error {
	db := stdlib.OpenDBFromPool(pool)
	defer db.Close()
	return migrate.Down(log, db, fsys, tableName)
}

// DownBy migrates the database down by i.
func DownBy(log *slog.Logger, pool *pgxpool.Pool, fsys fs.FS, tableName string, i int) error {
	db := stdlib.OpenDBFromPool(pool)
	defer db.Close()
	return migrate.DownBy(log, db, fsys, tableName, i)
}

// Redo redoes the latest migration.
func Redo(log *slog.Logger, pool *pgxpool.Pool, fsys fs.FS, tableName string) error {
	db := stdlib.OpenDBFromPool(pool)
	defer db.Close()
	return migrate.Redo(log, db, fsys, tableName)
}
