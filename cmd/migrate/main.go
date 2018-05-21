package main

import (
	"database/sql"
	"os"
	"strings"

	_ "github.com/lib/pq"

	"github.com/apex/log"
	"github.com/apex/log/handlers/cli"
	"github.com/matthewmueller/commander"
	"github.com/matthewmueller/migrate"
)

func main() {
	log.SetHandler(cli.Default)
	migrate.Log = log.Log

	cli := commander.New("migrate", "Postgres migration CLI")
	dir := cli.Flag("dir", "directory").Default("db").String()

	{
		up := cli.Command("up", "migrate up")
		db := up.Flag("db", "database url (e.g. postgres://localhost:5432)").Required().String()
		n := up.Arg("n", "migrate up by n").Uint()
		up.Run(func() error {
			conn, err := connect(*db)
			if err != nil {
				return err
			}
			defer conn.Close()

			// TODO: cleanup
			if *n == 0 {
				n = nil
			}

			return migrate.Up(conn, *dir, n)
		})
	}

	{
		down := cli.Command("down", "migrate down")
		db := down.Flag("db", "database url (e.g. postgres://localhost:5432)").Required().String()
		n := down.Arg("n", "migrate down by n").Uint()
		down.Run(func() error {
			conn, err := connect(*db)
			if err != nil {
				return err
			}
			defer conn.Close()

			// TODO: cleanup
			if *n == 0 {
				n = nil
			}

			return migrate.Down(conn, *dir, n)
		})
	}

	{
		create := cli.Command("create", "create migration files")
		name := create.Arg("name", "name of the migration").Required().Strings()
		create.Run(func() error {
			return migrate.Create(*dir, strings.Join(*name, "_"))
		})
	}

	{
		info := cli.Command("info", "get the current migration number")
		db := info.Flag("db", "database url (e.g. postgres://localhost:5432)").Required().String()
		info.Run(func() error {
			conn, err := connect(*db)
			if err != nil {
				return err
			}
			defer conn.Close()

			v, err := migrate.Version(conn)
			if err != nil {
				return err
			}

			log.Infof("currently at: %d", v)
			return nil
		})
	}

	if err := cli.Parse(os.Args[1:]); err != nil {
		log.Fatal(err.Error())
	}
}

func connect(url string) (*sql.DB, error) {
	db, err := sql.Open("postgres", url)
	if err != nil {
		return nil, err
	}

	if err := db.Ping(); err != nil {
		return nil, err
	}

	return db, nil
}
