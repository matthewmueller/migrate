package cli

import (
	"context"

	"github.com/livebud/cli"
	"github.com/matthewmueller/migrate/internal/txmigrate"
)

type down struct {
	N *int
}

func (in *down) Command(cmd cli.Command) cli.Command {
	cmd = cmd.Command("down", "migrate down")
	cmd.Arg("n", "go down by n").Optional().Int(&in.N)
	return cmd
}

func (c *CLI) Down(ctx context.Context, in *down) error {
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

	// be a bit extra careful here
	switch {
	case in.N == nil:
		return txmigrate.Down(log, db, fsys, c.tableName)
	case *in.N > 0:
		return txmigrate.DownBy(log, db, fsys, c.tableName, *in.N)
	}
	return nil
}
