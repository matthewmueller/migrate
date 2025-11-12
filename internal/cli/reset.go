package cli

import (
	"context"

	"github.com/livebud/cli"
	"github.com/matthewmueller/migrate/internal/txmigrate"
)

type reset struct {
}

func (in *reset) Command(cmd cli.Command) cli.Command {
	cmd = cmd.Command("reset", "reset all migrations")
	return cmd
}

func (c *CLI) Reset(ctx context.Context, in *reset) (err error) {
	// Connect to the database
	db, err := c.dialDb()
	if err != nil {
		return err
	}
	defer db.Close()

	log, err := c.log()
	if err != nil {
		return err
	}

	fsys, err := c.migrateFs()
	if err != nil {
		return err
	}

	return txmigrate.Reset(log, db, fsys, c.tableName)
}
