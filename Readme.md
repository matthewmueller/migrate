# migrate

[![GoDoc](https://godoc.org/github.com/matthewmueller/migrate?status.svg)](https://godoc.org/github.com/matthewmueller/migrate)

No-frills migration utility for PostgreSQL and SQLite.

## Features

- Stable and in use across 10+ projects over 3 years.
- Supports migrations from a [virtual file-system](https://github.com/matthewmueller/migrate/blob/bfacd7c1d10ef75d68406eab8e389384f9771a81/migrate_test.go#L50-L72)
- Supports several SQLite3 modules (full-text search, json, foreign keys, etc.)

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
      --db=DB            database url (e.g. 'postgres://localhost:5432/db')

Commands:

  help                 Show help for a command.
  new                  create a new migration
  up                   migrate up
  down                 migrate down
  reset                reset all down then up migrations
  redo                 redo the last migration
  info                 info on the current migration
```

## Help Wanted

- Generic driver interface

## Authors

- Matt Mueller [https://twitter.com/mattmueller](https://twitter.com/mattmueller)

## Running Tests

The tests depend on a local PostgreSQL database being present. Make sure you have PostgreSQL installed and then run the following from your terminal:

```sh
createdb migrate-test
```

Then you should be able to run:

```sh
make test
```

## License

MIT
