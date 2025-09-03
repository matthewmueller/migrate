package cli

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/livebud/cli"
	"github.com/matthewmueller/logs"
	"github.com/xo/dburl"

	// supported libraries
	_ "github.com/jackc/pgx/v5/stdlib"
	_ "github.com/mattn/go-sqlite3"
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
		return nil, errors.New("missing --db or $DATABASE_URL environment variable")
	}
	url, err := dburl.Parse(c.dbUrl)
	if err != nil {
		return nil, err
	}
	switch url.Scheme {
	case "postgres":
		return sql.Open("pgx", url.DSN)
	case "sqlite", "sqlite3":
		return sql.Open("sqlite3", url.DSN)
	default:
		return nil, fmt.Errorf("migrate doesn't support this url scheme: %s", url.Scheme)
	}
}

func (c *CLI) log() (*logs.Logger, error) {
	lvl, err := logs.ParseLevel(c.logLevel)
	if err != nil {
		return nil, err
	}
	return logs.New(logs.Filter(lvl, logs.Console(c.Stderr))), nil
}

// findFirstDir returns the first directory that exists in the list of paths
func findFirstDir(paths ...string) (string, error) {
	for _, path := range paths {
		if path == "" {
			continue
		}
		if stat, err := os.Stat(path); err == nil && stat.IsDir() {
			return path, nil
		}
	}
	return "", fmt.Errorf("unable to find migration directory in %v", paths)
}

func resolveDir(base, dir string) string {
	if filepath.IsAbs(dir) {
		return dir
	}
	return filepath.Join(base, dir)
}

func (c *CLI) findMigrateDir() (string, error) {
	// First check if the user provided a directory
	if c.migrateDir != "" {
		migrateDir := resolveDir(c.Dir, c.migrateDir)
		if _, err := os.Stat(migrateDir); err != nil {
			if os.IsNotExist(err) {
				return "", fmt.Errorf("%s/ directory doesn't exist", migrateDir)
			}
			return "", err
		}
		return migrateDir, nil
	}
	// Otherwise look in a few other default locations
	return findFirstDir(
		filepath.Join(c.Dir, "migrate"),
		filepath.Join(c.Dir, "internal", "migrate"),
	)
}

func (c *CLI) migrateFs() (fs.FS, error) {
	migrateDir, err := c.findMigrateDir()
	if err != nil {
		return nil, err
	}
	return os.DirFS(migrateDir), nil
}

func (c *CLI) Parse(ctx context.Context, args ...string) error {
	cli := cli.New("migrate", "No frills database migration CLI for Postgres & SQLite")
	cli.Flag("log", "log level").Enum(&c.logLevel, "debug", "info", "warn", "error").Default("info")
	cli.Flag("dir", "migrations directory").String(&c.migrateDir).Default("")
	cli.Flag("table", "table name").String(&c.tableName).Default("migrate")
	cli.Flag("db", "database connection string").Env("DATABASE_URL").String(&c.dbUrl).Default("")

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

	{ // Version
		in := &version{}
		cmd := in.Command(cli)
		cmd.Run(func(ctx context.Context) error { return c.Version(ctx, in) })
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
