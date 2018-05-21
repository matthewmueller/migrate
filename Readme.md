# migrate

No-frills migration utility for postgres

## Installation

```
go get github.com/matthewmueller/migrate
```

## Usage

```
usage: migrate [<flags>] <command> [<args> ...]

Postgres migration CLI

Flags:
  --help      Show context-sensitive help (also try --help-long and --help-man).
  --dir="db"  directory

Commands:
  help [<command>...]
    Show help.

  up --db=DB [<n>]
    migrate up

  down --db=DB [<n>]
    migrate down

  create <name>...
    create migration files

  info --db=DB
    get the current migration number
```

## License

MIT