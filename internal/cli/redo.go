package cli

import (
	"context"

	"github.com/livebud/cli"
	"github.com/matthewmueller/migrate"
)

type redo struct {
}

func (in *redo) Command(cmd cli.Command) cli.Command {
	cmd = cmd.Command("redo", "redo the last migration")
	return cmd
}

func (c *CLI) Redo(ctx context.Context, in *redo) error {
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

	if err := migrate.DownBy(log, db, fsys, c.tableName, 1); err != nil {
		return err
	}
	if err := migrate.UpBy(log, db, fsys, c.tableName, 1); err != nil {
		return err
	}

	return nil
}
