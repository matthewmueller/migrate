package migrate_test

import (
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"

	"golang.org/x/tools/godoc/vfs/httpfs"

	"github.com/apex/log"
	"github.com/apex/log/handlers/discard"
	"github.com/matthewmueller/migrate"
	"github.com/tj/assert"
	"golang.org/x/tools/godoc/vfs/mapfs"

	_ "github.com/lib/pq"
)

const tableName = "migrate"

var logger = func() log.Interface {
	return &log.Logger{
		Handler: discard.New(),
		Level:   log.InfoLevel,
	}
}()

func TestPostgres(t *testing.T) {
	url := "postgres://localhost:5432/migrate-test?sslmode=disable"
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.fn(t, url)
		})
	}
}

func TestSQLite(t *testing.T) {
	url := "sqlite:///tmp.db"
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.fn(t, url)
		})
	}
}

func connect(t testing.TB, url string) (*sql.DB, func()) {
	db, err := migrate.Connect(url)
	assert.NoError(t, err)
	return db, func() {
		assert.NoError(t, db.Close())
	}
}

func drop(t testing.TB, url string) {
	db, close := connect(t, url)
	defer close()
	_, err := db.Query(`
		drop table if exists migrate;
		drop table if exists users;
		drop table if exists teams;
	`)
	assert.NoError(t, err)
}

func exists(t testing.TB, path string) {
	_, err := os.Stat(path)
	assert.NoError(t, err)
}

func notExists(err error, name string) bool {
	return strings.Contains(err.Error(), fmt.Sprintf("relation \"%s\" does not exist", name)) ||
		strings.Contains(err.Error(), fmt.Sprintf("no such table: %s", name))
}

func syntaxError(err error, name string) bool {
	return strings.Contains(err.Error(), fmt.Sprintf(`syntax error at or near "%s"`, name)) ||
		strings.Contains(err.Error(), fmt.Sprintf(`near "%s": syntax error`, name))
}

var tests = []struct {
	name string
	fn   func(t testing.TB, url string)
}{
	{
		name: "zero",
		fn: func(t testing.TB, url string) {
			drop(t, url)
			fs := httpfs.New(mapfs.New(map[string]string{
				"000_init.up.sql": `
					create table if not exists teams (
						id serial primary key not null,
						name text not null
					);
					create table if not exists users (
						id serial primary key not null,
						email text not null,
						created_at time with time zone not null,
						updated_at time with time zone not null
					);
				`,
				"000_init.down.sql": `
					drop table if exists users;
					drop table if exists teams;
				`,
			}))
			db, close := connect(t, url)
			defer close()
			err := migrate.Up(logger, db, fs, tableName)
			assert.Equal(t, migrate.ErrZerothMigration, err)
		},
	},
	{
		name: "up down",
		fn: func(t testing.TB, url string) {
			drop(t, url)

			fs := httpfs.New(mapfs.New(map[string]string{
				"001_init.up.sql": `
					create table if not exists teams (
						id serial primary key not null,
						name text not null
					);
					create table if not exists users (
						id serial primary key not null,
						email text not null,
						created_at time with time zone not null,
						updated_at time with time zone not null
					);
				`,
				"001_init.down.sql": `
					drop table if exists users;
					drop table if exists teams;
				`,
			}))

			db, close := connect(t, url)
			defer close()

			err := migrate.Up(logger, db, fs, tableName)
			assert.NoError(t, err)

			rows, err := db.Query(`insert into teams (id, name) values (1, 'jack')`)
			assert.NoError(t, err)
			for rows.Next() {
				var id int
				var name string
				err := rows.Scan(&id, &name)
				assert.NoError(t, err)
				assert.Equal(t, 1, id)
				assert.Equal(t, "jack", name)
			}
			assert.NoError(t, rows.Err())

			err = migrate.Down(logger, db, fs, tableName)
			assert.NoError(t, err)

			rows, err = db.Query(`insert into teams (id, name) values (2, 'jack')`)
			assert.Contains(t, err.Error(), "teams")
			assert.True(t, notExists(err, "teams"), err.Error())
		},
	},
	{
		name: "upupdowndown",
		fn: func(t testing.TB, url string) {
			drop(t, url)

			fs := httpfs.New(mapfs.New(map[string]string{
				"001_init.up.sql": `
					create table if not exists teams (
						id serial primary key not null,
						name text not null
					);
				`,
				"001_init.down.sql": `
					drop table if exists teams;
				`,
				"002_users.up.sql": `
					create table if not exists users (
						id serial primary key not null,
						email text not null
					);
				`,
				"002_users.down.sql": `
					drop table if exists users;
				`,
			}))

			db, close := connect(t, url)
			defer close()

			err := migrate.Up(logger, db, fs, tableName)
			assert.NoError(t, err)
			err = migrate.Up(logger, db, fs, tableName)
			assert.NoError(t, err)

			rows, err := db.Query(`insert into users (id, email) values (1, 'jack')`)
			assert.NoError(t, err)
			for rows.Next() {
				var id int
				var email string
				err := rows.Scan(&id, &email)
				assert.NoError(t, err)
				assert.Equal(t, 1, id)
				assert.Equal(t, "jack", email)
			}
			assert.NoError(t, rows.Err())

			err = migrate.Down(logger, db, fs, tableName)
			assert.NoError(t, err)
			err = migrate.Down(logger, db, fs, tableName)
			assert.NoError(t, err)

			rows, err = db.Query(`insert into users (id, email) values (2, 'jack')`)
			assert.Contains(t, err.Error(), "users")
			assert.True(t, notExists(err, "users"), err.Error())
		},
	},
	{
		name: "upbydownby",
		fn: func(t testing.TB, url string) {
			drop(t, url)

			fs := httpfs.New(mapfs.New(map[string]string{
				"001_init.up.sql": `
						create table if not exists teams (
							id serial primary key not null,
							name text not null
						);
					`,
				"001_init.down.sql": `
						drop table if exists teams;
					`,
				"002_users.up.sql": `
						create table if not exists users (
							id serial primary key not null,
							email text not null
						);
					`,
				"002_users.down.sql": `
						drop table if exists users;
					`,
			}))

			db, close := connect(t, url)
			defer close()

			err := migrate.UpBy(logger, db, fs, tableName, 1)
			assert.NoError(t, err)

			_, err = db.Query(`insert into teams (name) values ('jack')`)
			assert.NoError(t, err)
			_, err = db.Query(`insert into users (email) values ('jack')`)
			assert.Contains(t, err.Error(), "users")
			assert.True(t, notExists(err, "users"), err.Error())

			err = migrate.UpBy(logger, db, fs, tableName, 1)
			assert.NoError(t, err)
			_, err = db.Query(`insert into teams (name) values ('jack')`)
			assert.NoError(t, err)
			_, err = db.Query(`insert into users (email) values ('jack')`)
			assert.NoError(t, err)

			err = migrate.UpBy(logger, db, fs, tableName, 1)
			assert.NoError(t, err)
			assert.NoError(t, err)
			_, err = db.Query(`insert into teams (name) values ('jack')`)
			assert.NoError(t, err)
			_, err = db.Query(`insert into users (email) values ('jack')`)
			assert.NoError(t, err)

			err = migrate.DownBy(logger, db, fs, tableName, 1)
			assert.NoError(t, err)
			_, err = db.Query(`insert into teams (name) values ('jack')`)
			assert.NoError(t, err)
			_, err = db.Query(`insert into users (email) values ('jack')`)
			assert.Contains(t, err.Error(), "users")
			assert.True(t, notExists(err, "users"), err.Error())

			err = migrate.DownBy(logger, db, fs, tableName, 1)
			assert.NoError(t, err)
			_, err = db.Query(`insert into teams (name) values ('jack')`)
			assert.Contains(t, err.Error(), "teams")
			assert.True(t, notExists(err, "teams"), err.Error())
			_, err = db.Query(`insert into users (email) values ('jack')`)
			assert.Contains(t, err.Error(), "users")
			assert.True(t, notExists(err, "users"), err.Error())

			err = migrate.DownBy(logger, db, fs, tableName, 1)
			assert.NoError(t, err)
			_, err = db.Query(`insert into teams (name) values ('jack')`)
			assert.Contains(t, err.Error(), "teams")
			assert.True(t, notExists(err, "teams"), err.Error())
			_, err = db.Query(`insert into users (email) values ('jack')`)
			assert.Contains(t, err.Error(), "users")
			assert.True(t, notExists(err, "users"), err.Error())
		},
	},
	{
		name: "uprollback",
		fn: func(t testing.TB, url string) {
			drop(t, url)

			fs := httpfs.New(mapfs.New(map[string]string{
				"001_init.up.sql": `
						create table if not exists teams (
							id serial primary key not null,
							name text not null
						);
					`,
				"001_init.down.sql": `
						drop table if exists teams;
					`,
				"002_users.up.sql": `
						create table if not exists users (
							id serial primary key not null -- intentionally missing comma
							email text not null
						);
					`,
				"002_users.down.sql": `
						drop table if exists users;
					`,
			}))

			db, close := connect(t, url)
			defer close()

			err := migrate.UpBy(logger, db, fs, tableName, 1)
			assert.NoError(t, err)

			_, err = db.Query(`insert into teams (name) values ('jack')`)
			assert.NoError(t, err)
			_, err = db.Query(`insert into users (email) values ('jack')`)
			assert.Contains(t, err.Error(), "users")
			assert.True(t, notExists(err, "users"), err.Error())

			err = migrate.UpBy(logger, db, fs, tableName, 1)
			assert.NotNil(t, err)
			assert.True(t, syntaxError(err, "email"), err.Error())

			_, err = db.Query(`insert into teams (name) values ('jack')`)
			assert.NoError(t, err)
			_, err = db.Query(`insert into users (email) values ('jack')`)
			assert.Contains(t, err.Error(), "users")
			assert.True(t, notExists(err, "users"), err.Error())
		},
	},
	{
		name: "downrollback",
		fn: func(t testing.TB, url string) {
			drop(t, url)

			fs := httpfs.New(mapfs.New(map[string]string{
				"001_init.up.sql": `
						create table if not exists teams (
							id serial primary key not null,
							name text not null
						);
					`,
				"001_init.down.sql": `
						drop table if exis teams; -- intentional syntax error
					`,
				"002_users.up.sql": `
						create table if not exists users (
							id serial primary key not null,
							email text not null
						);
					`,
				"002_users.down.sql": `
						drop table if exists users;
					`,
			}))

			db, close := connect(t, url)
			defer close()

			// setup
			err := migrate.Up(logger, db, fs, tableName)
			assert.NoError(t, err)

			err = migrate.DownBy(logger, db, fs, tableName, 1)
			assert.NoError(t, err)
			_, err = db.Query(`insert into teams (name) values ('jack')`)
			assert.NoError(t, err)
			_, err = db.Query(`insert into users (email) values ('jack')`)
			assert.Contains(t, err.Error(), "users")
			assert.True(t, notExists(err, "users"))

			err = migrate.DownBy(logger, db, fs, tableName, 1)
			assert.NotNil(t, err)
			assert.True(t, syntaxError(err, "exis"), err.Error())

			_, err = db.Query(`insert into teams (name) values ('jack')`)
			assert.NoError(t, err)
		},
	},
	{
		name: "new",
		fn: func(t testing.TB, url string) {
			drop(t, url)

			// cleanup
			assert.NoError(t, os.RemoveAll("migrate"))

			err := migrate.New(logger, "migrate", "setup")
			assert.NoError(t, err)
			exists(t, "migrate/001_setup.up.sql")
			exists(t, "migrate/001_setup.down.sql")

			err = migrate.New(logger, "migrate", "create teams")
			assert.NoError(t, err)
			exists(t, "migrate/002_create_teams.up.sql")
			exists(t, "migrate/002_create_teams.down.sql")

			err = migrate.New(logger, "migrate", "new-users")
			assert.NoError(t, err)
			exists(t, "migrate/003_new_users.up.sql")
			exists(t, "migrate/003_new_users.down.sql")

			if !t.Failed() {
				assert.NoError(t, os.RemoveAll("migrate"))
			}
		},
	},
	{
		name: "remoteversion",
		fn: func(t testing.TB, url string) {
			drop(t, url)

			fs := httpfs.New(mapfs.New(map[string]string{
				"001_init.up.sql": `
						create table if not exists teams (
							id serial primary key not null,
							name text not null
						);
					`,
				"001_init.down.sql": `
						drop table if exists teams;
					`,
				"002_users.up.sql": `
						create table if not exists users (
							id serial primary key not null,
							email text not null
						);
					`,
				"002_users.down.sql": `
						drop table if exists users;
					`,
			}))

			db, close := connect(t, url)
			defer close()

			// setup
			err := migrate.Up(logger, db, fs, tableName)
			assert.NoError(t, err)

			name, err := migrate.RemoteVersion(db, fs, tableName)
			assert.NoError(t, err)
			assert.Equal(t, `002_users.up.sql`, name)

			// teardown
			err = migrate.Down(logger, db, fs, tableName)
			assert.NoError(t, err)

			name, err = migrate.RemoteVersion(db, fs, tableName)
			assert.Equal(t, migrate.ErrNoMigrations, err)
		},
	},
	{
		name: "localversion",
		fn: func(t testing.TB, url string) {
			drop(t, url)

			fs := httpfs.New(mapfs.New(map[string]string{
				"001_init.up.sql": `
						create table if not exists teams (
							id serial primary key not null,
							name text not null
						);
					`,
				"001_init.down.sql": `
						drop table if exists teams;
					`,
				"002_users.up.sql": `
						create table if not exists users (
							id serial primary key not null,
							email text not null
						);
					`,
				"002_users.down.sql": `
						drop table if exists users;
					`,
			}))

			name, err := migrate.LocalVersion(fs)
			assert.NoError(t, err)
			assert.Equal(t, `002_users.up.sql`, name)
		},
	},
}
