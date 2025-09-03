package cli

import (
	"context"
	"fmt"

	"github.com/livebud/cli"
	"github.com/matthewmueller/migrate"
)

type version struct {
}

func (in *version) Command(cmd cli.Command) cli.Command {
	cmd = cmd.Command("version", "print the current version")
	return cmd
}

func (c *CLI) Version(ctx context.Context, in *version) error {
	fmt.Fprintln(c.Stdout, "v"+migrate.Version())
	return nil
}
