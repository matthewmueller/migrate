package main

import (
	"context"
	"os"

	"github.com/apex/log"
	"github.com/apex/log/handlers/cli"
	"github.com/jackc/pgx"
	"github.com/matthewmueller/commander"
	"github.com/matthewmueller/migrate"
)

func main() {
	log.SetHandler(cli.Default)
	migrate.Log = log.Log

	cli := commander.New("migrate", "Postgres migration CLI")

	{
		new := cli.Command("new", "create a new migration")
		name := new.Arg("name", "create a new migration by name").Required().String()
		dir := new.Flag("dir", "migrations directory").Default("./migrations").String()
		new.Run(func() error {
			return migrate.New(*dir, *name)
		})
	}

	{
		up := cli.Command("up", "migrate up")
		db := up.Flag("db", "database url (e.g. postgres://localhost:5432)").Required().String()
		name := up.Arg("name", "name of the migration to migrate up to").String()
		dir := up.Flag("dir", "migrations directory").Default("./migrations").String()
		up.Run(func() error {
			conn, err := connect(*db)
			if err != nil {
				return err
			}
			defer conn.Close()

			var n string
			if name != nil {
				n = *name
			}

			return migrate.Up(conn, *dir, n)
		})
	}

	{
		down := cli.Command("down", "migrate down")
		db := down.Flag("db", "database url (e.g. postgres://localhost:5432)").Required().String()
		name := down.Arg("name", "name of the migration to migrate down to").String()
		dir := down.Flag("dir", "migrations directory").Default("./migrations").String()
		down.Run(func() error {
			conn, err := connect(*db)
			if err != nil {
				return err
			}
			defer conn.Close()

			var n string
			if name != nil {
				n = *name
			}

			return migrate.Down(conn, *dir, n)
		})
	}

	{
		embed := cli.Command("embed", "embed the migrations into an runnable module")
		dir := embed.Flag("dir", "migrations directory").Default("./migrations").String()
		embed.Run(func() error {
			return migrate.Embed(dir)
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

func connect(url string) (*pgx.Conn, error) {
	cfg, err := pgx.ParseConnectionString(url)
	if err != nil {
		return nil, err
	}

	conn, err := pgx.Connect(cfg)
	if err != nil {
		return nil, err
	}

	if err := conn.Ping(context.TODO()); err != nil {
		return nil, err
	}

	return conn, nil
}
