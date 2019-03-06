package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"os"

	"github.com/apex/log"
	"github.com/apex/log/handlers/cli"
	"github.com/matthewmueller/commander"
	"github.com/matthewmueller/migrate"
)

func main() {
	log := &log.Logger{
		Handler: cli.Default,
		Level:   log.InfoLevel,
	}

	todo := context.TODO()

	cli := commander.New("migrate", "Postgres migration CLI")
	dir := cli.Flag("dir", "migrations directory").Default("./migrate").String()
	table := cli.Flag("table", "table name").Default("migrate").String()

	{
		new := cli.Command("new", "create a new migration")
		name := new.Arg("name", "create a new migration by name").Required().String()
		new.Run(func() error { return migrate.New(log, *dir, *name) })
	}

	{ // migrate up
		up := cli.Command("up", "migrate up")
		db := up.Arg("db", "database url (e.g. postgres://localhost:5432)").Required().String()
		n := up.Arg("n", "go up by n").Int()
		up.Run(func() error {
			conn, err := connect(todo, *db)
			if err != nil {
				return err
			}
			defer conn.Close()
			if n != nil {
				return migrate.UpBy(log, conn, http.Dir(*dir), *table, *n)
			}
			return migrate.Up(log, conn, http.Dir(*dir), *table)
		})
	}

	{ // migrate down
		down := cli.Command("down", "migrate down")
		db := down.Flag("db", "database url (e.g. postgres://localhost:5432)").Required().String()
		n := down.Arg("n", "go up by n").Int()
		down.Run(func() error {
			db, err := connect(todo, *db)
			if err != nil {
				return err
			}
			defer db.Close()
			if n != nil {
				return migrate.UpBy(log, db, http.Dir(*dir), *table, *n)
			}
			return migrate.Up(log, db, http.Dir(*dir), *table)
		})
	}

	{ // info about the current migration
		info := cli.Command("info", "info on the current migration")
		db := info.Arg("db", "database url (e.g. postgres://localhost:5432)").Required().String()
		info.Run(func() error {
			db, err := connect(todo, *db)
			if err != nil {
				return err
			}
			defer db.Close()

			local, err := migrate.LocalVersion(http.Dir(*dir))
			if err == migrate.ErrNoMigrations {
				return errors.New("no local migrations yet")
			} else if os.IsNotExist(err) {
				return fmt.Errorf("%s doesn't exist", *dir)
			} else if err != nil {
				return err
			}

			remote, err := migrate.RemoteVersion(db, http.Dir(*dir), *table)
			if err == migrate.ErrNoMigrations {
				return errors.New("no remote migrations yet")
			} else if os.IsNotExist(err) {
				return fmt.Errorf("%s doesn't exist", *dir)
			} else if err != nil {
				return err
			}

			log.Infof("local: %s", local)
			log.Infof("remote: %s", remote)
			return nil
		})
	}

	if err := cli.Parse(os.Args[1:]); err != nil {
		log.Fatal(err.Error())
	}
}

func connect(ctx context.Context, url string) (*sql.DB, error) {
	db, err := sql.Open("postgres", url)
	if err != nil {
		return nil, err
	}
	if err := db.PingContext(ctx); err != nil {
		return nil, err
	}
	return db, nil
}
