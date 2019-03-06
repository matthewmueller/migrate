# migrate

[![GoDoc](https://godoc.org/github.com/matthewmueller/migrate?status.svg)](https://godoc.org/github.com/matthewmueller/migrate)

No-frills migration utility for postgres.

## Installation

```
go get github.com/matthewmueller/migrate
```

## Usage

```
Usage:

  migrate [<flags>] <command> [<args> ...]

Flags:

  -h, --help             Output usage information.
      --dir="./migrate"  migrations directory
      --table="migrate"  table name

Commands:

  help                 Show help for a command.
  new                  create a new migration
  up                   migrate up
  down                 migrate down
  info                 info on the current migration
```

## TODO

- Generic drivers
- Support embedded migrations

## Authors

- Matt Mueller [https://twitter.com/mattmueller](https://twitter.com/mattmueller)

## License

MIT
