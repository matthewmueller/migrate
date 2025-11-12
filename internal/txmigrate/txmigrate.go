package txmigrate

import (
	"database/sql"
	"io/fs"
	"log/slog"

	"github.com/matthewmueller/migrate"
)

func Up(_ *slog.Logger, db *sql.DB, fsys fs.FS, table string) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	err = migrate.Up(nil, tx, fsys, table)
	if err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

func UpBy(_ *slog.Logger, db *sql.DB, fsys fs.FS, table string, i int) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	err = migrate.UpBy(nil, tx, fsys, table, i)
	if err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

func Down(_ *slog.Logger, db *sql.DB, fsys fs.FS, table string) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	err = migrate.Down(nil, tx, fsys, table)
	if err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

func DownBy(_ *slog.Logger, db *sql.DB, fsys fs.FS, table string, i int) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	err = migrate.DownBy(nil, tx, fsys, table, i)
	if err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

func Redo(_ *slog.Logger, db *sql.DB, fsys fs.FS, table string) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	err = migrate.Redo(nil, tx, fsys, table)
	if err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

func Reset(_ *slog.Logger, db *sql.DB, fsys fs.FS, table string) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	err = migrate.Reset(nil, tx, fsys, table)
	if err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

func RemoteVersion(db *sql.DB, fsys fs.FS, table string) (string, error) {
	tx, err := db.Begin()
	if err != nil {
		return "", err
	}
	name, err := migrate.RemoteVersion(tx, fsys, table)
	if err != nil {
		tx.Rollback()
		return "", err
	} else if err := tx.Commit(); err != nil {
		return "", err
	}
	return name, nil
}
