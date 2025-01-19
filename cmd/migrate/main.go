package main

import (
	"os"

	"github.com/matthewmueller/migrate/internal/cli"
)

func main() {
	os.Exit(cli.Run())
}
