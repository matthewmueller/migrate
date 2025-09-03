package cli

import (
	"context"
	"os"

	"github.com/Bowery/prompt"
	"github.com/livebud/cli"
	"github.com/matthewmueller/migrate"
	"github.com/matthewmueller/virt"
)

type newIn struct {
	Name string
}

func (in *newIn) Command(cmd cli.Command) cli.Command {
	cmd = cmd.Command("new", "create a new migration")
	cmd.Arg("name", "create a new migration by name").String(&in.Name).Default("")
	return cmd
}

func (c *CLI) New(ctx context.Context, in *newIn) (err error) {
	if in.Name == "" {
		in.Name, err = prompt.Basic("Migration name?", true)
		if err != nil {
			return err
		}
	}
	log, err := c.log()
	if err != nil {
		return err
	}

	// Create the migrate directory if it doesn't exist
	migrateDir := c.migrateDir
	if c.migrateDir == "" {
		migrateDir = "migrate"
	}
	migrateDir = resolveDir(c.Dir, migrateDir)
	if err := os.MkdirAll(migrateDir, 0755); err != nil {
		return err
	}

	return migrate.New(log, virt.OS(migrateDir), in.Name)
}
