package cli

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/livebud/cli"
	"github.com/matthewmueller/logs"
)

func Run() int {
	ctx := context.Background()
	cli := Default()
	if err := cli.Parse(ctx, os.Args[1:]...); err != nil {
		logs.Error(err.Error())
		return 1
	}
	return 0
}

func Default() *CLI {
	return &CLI{
		Stdout: os.Stdout,
		Stderr: os.Stderr,
		Stdin:  os.Stdin,
		Env:    os.Environ(),
		Dir:    ".",
	}
}

type CLI struct {
	Stdout io.Writer
	Stderr io.Writer
	Stdin  io.Reader
	Env    []string
	Dir    string

	// Filled in after parsing
	logLevel   string
	migrateDir string
	tableName  string
	dbUrl      string
}

func (c *CLI) dialDb() (*sql.DB, error) {
	if c.dbUrl == "" {
		return nil, errors.New("missing --db=<url> flag or DATABASE_URL environment variable")
	}
	return sql.Open("postgres", c.dbUrl)
}

func (c *CLI) log() (*logs.Logger, error) {
	lvl, err := logs.ParseLevel(c.logLevel)
	if err != nil {
		return nil, err
	}
	return logs.New(logs.Filter(lvl, logs.Console(c.Stderr))), nil
}

func (c *CLI) migrateFs() (fs.FS, error) {
	migrateDir := filepath.Join(c.Dir, c.migrateDir)
	if _, err := os.Stat(migrateDir); err != nil {
		return nil, err
	}
	return os.DirFS(migrateDir), nil
}

func (c *CLI) Parse(ctx context.Context, args ...string) error {
	cli := cli.New("migrate", "No frills database migration CLI for Postgres & SQLite")
	cli.Flag("log", "log level").Enum(&c.logLevel, "debug", "info", "warn", "error").Default("info")
	cli.Flag("dir", "migrations directory").String(&c.migrateDir).Default("./migrate")
	cli.Flag("table", "table name").String(&c.tableName).Default("migrate")
	cli.Flag("db", "database url (e.g. postgres://localhost:5432/db)").String(&c.dbUrl).Default("")

	{ // New
		in := &newIn{}
		cmd := in.Command(cli)
		cmd.Run(func(ctx context.Context) error { return c.New(ctx, in) })
	}

	{ // Up
		in := &up{}
		cmd := in.Command(cli)
		cmd.Run(func(ctx context.Context) error { return c.Up(ctx, in) })
	}

	{ // Down
		in := &down{}
		cmd := in.Command(cli)
		cmd.Run(func(ctx context.Context) error { return c.Down(ctx, in) })
	}

	{ // Reset
		in := &reset{}
		cmd := in.Command(cli)
		cmd.Run(func(ctx context.Context) error { return c.Reset(ctx, in) })
	}

	{ // Redo
		in := &redo{}
		cmd := in.Command(cli)
		cmd.Run(func(ctx context.Context) error { return c.Redo(ctx, in) })
	}

	{ // Info
		in := &info{}
		cmd := in.Command(cli)
		cmd.Run(func(ctx context.Context) error { return c.Info(ctx, in) })
	}

	// Run the CLI
	if err := cli.Parse(ctx, args...); err != nil {
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return err
	}

	return nil
}
