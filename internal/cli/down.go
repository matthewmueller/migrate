package cli

import (
	"context"

	"github.com/livebud/cli"
	"github.com/matthewmueller/migrate"
)

type down struct {
	N int
}

func (in *down) Command(cmd cli.Command) cli.Command {
	cmd = cmd.Command("down", "migrate down")
	cmd.Arg("n", "go down by n").Int(&in.N)
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
	case in.N == 0:
		return migrate.Down(log, db, fsys, c.tableName)
	case in.N > 0:
		return migrate.DownBy(log, db, fsys, c.tableName, in.N)
	}
	return nil
}
