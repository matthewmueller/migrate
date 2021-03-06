package main

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/apex/log"
	"github.com/apex/log/handlers/cli"
	"github.com/matthewmueller/commander"
	"github.com/matthewmueller/migrate"
	prompt "github.com/tj/go-prompt"

	_ "github.com/lib/pq"
)

func main() {
	log := &log.Logger{
		Handler: cli.Default,
		Level:   log.InfoLevel,
	}

	cli := commander.New("migrate", "No frills database migration CLI for Postgres & SQLite")
	dir := cli.Flag("dir", "migrations directory").Default("./migrate").String()
	table := cli.Flag("table", "table name").Default("migrate").String()

	{
		new := cli.Command("new", "create a new migration")
		name := new.Arg("name", "create a new migration by name").String()
		new.Run(func() error {
			if *name != "" {
				return migrate.New(log, *dir, *name)
			}
			var name string
		askName:
			name = prompt.StringRequired("  • Migration name? ")
			if len(strings.TrimSpace(name)) == 0 {
				goto askName
			}
			return migrate.New(log, *dir, name)
		})
	}

	{ // migrate up
		up := cli.Command("up", "migrate up")
		db := up.Arg("db", "database url (e.g. postgres://localhost:5432)").Required().String()
		n := up.Arg("n", "go up by n").Int()
		up.Run(func() error {
			db, err := migrate.Connect(*db)
			if err != nil {
				return err
			}
			defer db.Close()

			// be a bit extra careful here
			switch {
			case *n == 0:
				return migrate.Up(log, db, os.DirFS(*dir), *table)
			case *n > 0:
				return migrate.UpBy(log, db, os.DirFS(*dir), *table, *n)
			}
			return nil
		})
	}

	{ // migrate down
		down := cli.Command("down", "migrate down")
		db := down.Arg("db", "database url (e.g. postgres://localhost:5432)").Required().String()
		n := down.Arg("n", "go up by n").Int()
		down.Run(func() error {
			db, err := migrate.Connect(*db)
			if err != nil {
				return err
			}
			defer db.Close()

			// be a bit extra careful here
			switch {
			case *n == 0:
				return migrate.Down(log, db, os.DirFS(*dir), *table)
			case *n > 0:
				return migrate.DownBy(log, db, os.DirFS(*dir), *table, *n)
			}
			return nil
		})
	}

	{ // info about the current migration
		info := cli.Command("info", "info on the current migration")
		db := info.Arg("db", "database url (e.g. postgres://localhost:5432)").Required().String()
		info.Run(func() error {
			db, err := migrate.Connect(*db)
			if err != nil {
				return err
			}
			defer db.Close()

			local, err := migrate.LocalVersion(os.DirFS(*dir))
			if err == migrate.ErrNoMigrations {
				return errors.New("no local migrations yet")
			} else if os.IsNotExist(err) {
				return fmt.Errorf("%s doesn't exist", *dir)
			} else if err != nil {
				return err
			}

			remote, err := migrate.RemoteVersion(db, os.DirFS(*dir), *table)
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
