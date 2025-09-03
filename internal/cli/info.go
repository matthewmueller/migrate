package cli

import (
	"context"
	"errors"
	"fmt"

	"github.com/livebud/cli"
	"github.com/matthewmueller/migrate"
)

type info struct {
}

func (in *info) Command(cmd cli.Command) cli.Command {
	cmd = cmd.Command("info", "show information about the migrations")
	return cmd
}

func (c *CLI) Info(ctx context.Context, in *info) error {
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

	local, err := migrate.LocalVersion(fsys)
	if err == migrate.ErrNoMigrations {
		return errors.New("no local migrations yet")
	} else if err != nil {
		return err
	}

	remote, err := migrate.RemoteVersion(db, fsys, c.tableName)
	if err == migrate.ErrNoMigrations {
		return errors.New("no remote migrations yet")
	} else if err != nil {
		return err
	}
	fmt.Println(remote)

	log.Info("local: " + local)
	log.Info("remote: " + remote)
	return nil
}
