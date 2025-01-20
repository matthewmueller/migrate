# 0.0.2 / 2025-01-20

- only import database clients from cli, not from migrate library
- bump cli to v0.0.13
- modernize library
- remove ioutil

# 0.0.1 / 2024-01-27

- add redo, reset commands and DATABASE_URL envvar support

# 0.0.0 / 2021-09-14

- bump dependencies to fix migrate cli
- migrate: http.Filesystem => fs.FS
- build sqlite with some niceties
- Readme: Add note about SQLite
- migrate: update dependencies and fix lint warnings
- add go.mod
- remove accidental fmt.Println
- optional logger
- remove tmp.db before each test
- fix segfaults
- clean up after test
- remove commented out code
- add sqlite support
- update readme
- update readme
- fix table name when using migrate --table
- add slug casing to migrate new
- ready to open source
- finish up the CLI
- finish up new version and tests
- fix cli
- fix tests
- a bunch of breaking changes, this should be the final API. I don't really want to change this anymore.
- fix command
- tests passing, embed not working yet, but this isn't the most important thing
- use pgx
- fix the line numbers
- allow other types of files in our db and default dir to cwd rather than db
- add the makefile
- better error handling and logging
- add a quick readme
- finish initial migrate cli tool
