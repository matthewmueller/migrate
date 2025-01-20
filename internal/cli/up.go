package cli

import (
	"context"

	"github.com/livebud/cli"
	"github.com/matthewmueller/migrate"
)

type up struct {
	N *int
}

func (in *up) Command(cmd cli.Command) cli.Command {
	cmd = cmd.Command("up", "migrate up")
	cmd.Arg("n", "go up by n").Optional().Int(&in.N)
	return cmd
}

func (c *CLI) Up(ctx context.Context, in *up) error {
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
		return migrate.Up(log, db, fsys, c.tableName)
	case *in.N > 0:
		return migrate.UpBy(log, db, fsys, c.tableName, *in.N)
	}

	return nil
}
