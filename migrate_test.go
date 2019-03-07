package migrate_test

import (
	"database/sql"
	"os"
	"testing"

	"golang.org/x/tools/godoc/vfs/httpfs"

	"github.com/apex/log"
	"github.com/matthewmueller/migrate"
	"github.com/tj/assert"
	"golang.org/x/tools/godoc/vfs/mapfs"

	_ "github.com/lib/pq"
)

var url = "postgres://localhost:5432/migrate-test?sslmode=disable"
var tableName = "migrate"

func connect(t testing.TB) (*sql.DB, func()) {
	db, err := sql.Open("postgres", url)
	assert.NoError(t, err)
	return db, func() {
		assert.NoError(t, db.Close())
	}
}

func drop(t testing.TB) {
	db, close := connect(t)
	defer close()
	_, err := db.Query(`
		drop table if exists migrate;
		drop table if exists users;
		drop table if exists teams;
		drop extension if exists citext;
	`)
	assert.NoError(t, err)
}

func exists(t testing.TB, path string) {
	_, err := os.Stat(path)
	assert.NoError(t, err)
}

func TestZero(t *testing.T) {
	drop(t)

	log := log.Log
	fs := httpfs.New(mapfs.New(map[string]string{
		"000_init.up.sql": `
			create extension if not exists citext;

			create table if not exists teams (
				id serial primary key not null,
				name citext not null
			);

			create table if not exists users (
				id serial primary key not null,
				email citext not null,

				created_at time with time zone not null,
				updated_at time with time zone not null
			);
		`,
		"000_init.down.sql": `
			drop table if exists users;
			drop table if exists teams;
			drop extension if exists citext;
		`,
	}))

	db, close := connect(t)
	defer close()

	err := migrate.Up(log, db, fs, tableName)
	assert.Equal(t, migrate.ErrZerothMigration, err)
}

func TestUpDown(t *testing.T) {
	drop(t)

	log := log.Log
	fs := httpfs.New(mapfs.New(map[string]string{
		"001_init.up.sql": `
			create extension if not exists citext;

			create table if not exists teams (
				id serial primary key not null,
				name citext not null
			);

			create table if not exists users (
				id serial primary key not null,
				email citext not null,

				created_at time with time zone not null,
				updated_at time with time zone not null
			);
		`,
		"001_init.down.sql": `
			drop table if exists users;
			drop table if exists teams;
			drop extension if exists citext;
		`,
	}))

	db, close := connect(t)
	defer close()

	err := migrate.Up(log, db, fs, tableName)
	assert.NoError(t, err)

	rows, err := db.Query(`insert into teams (name) values ('jack') returning id, name`)
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

	err = migrate.Down(log, db, fs, tableName)
	assert.NoError(t, err)

	rows, err = db.Query(`insert into teams (name) values ('jack') returning id, name`)
	assert.Contains(t, err.Error(), "teams")
	assert.Contains(t, err.Error(), "does not exist")
}

func TestUpUpDownDown(t *testing.T) {
	drop(t)

	log := log.Log
	fs := httpfs.New(mapfs.New(map[string]string{
		"001_init.up.sql": `
			create extension if not exists citext;

			create table if not exists teams (
				id serial primary key not null,
				name citext not null
			);
		`,
		"001_init.down.sql": `
			drop table if exists teams;
			drop extension if exists citext;
		`,
		"002_users.up.sql": `
			create table if not exists users (
				id serial primary key not null,
				email citext not null
			);
		`,
		"002_users.down.sql": `
			drop table if exists users;
		`,
	}))

	db, close := connect(t)
	defer close()

	err := migrate.Up(log, db, fs, tableName)
	assert.NoError(t, err)
	err = migrate.Up(log, db, fs, tableName)
	assert.NoError(t, err)

	rows, err := db.Query(`insert into users (email) values ('jack') returning id, email`)
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

	err = migrate.Down(log, db, fs, tableName)
	assert.NoError(t, err)
	err = migrate.Down(log, db, fs, tableName)
	assert.NoError(t, err)

	rows, err = db.Query(`insert into users (email) values ('jack') returning id, email`)
	assert.Contains(t, err.Error(), "users")
	assert.Contains(t, err.Error(), "does not exist")
}

func TestUpByDownBy(t *testing.T) {
	drop(t)

	log := log.Log
	fs := httpfs.New(mapfs.New(map[string]string{
		"001_init.up.sql": `
			create extension if not exists citext;

			create table if not exists teams (
				id serial primary key not null,
				name citext not null
			);
		`,
		"001_init.down.sql": `
			drop table if exists teams;
			drop extension if exists citext;
		`,
		"002_users.up.sql": `
			create table if not exists users (
				id serial primary key not null,
				email citext not null
			);
		`,
		"002_users.down.sql": `
			drop table if exists users;
		`,
	}))

	db, close := connect(t)
	defer close()

	err := migrate.UpBy(log, db, fs, tableName, 1)
	assert.NoError(t, err)

	_, err = db.Query(`insert into teams (name) values ('jack') returning id, name`)
	assert.NoError(t, err)
	_, err = db.Query(`insert into users (email) values ('jack') returning id, email`)
	assert.Contains(t, err.Error(), "users")
	assert.Contains(t, err.Error(), "does not exist")

	err = migrate.UpBy(log, db, fs, tableName, 1)
	assert.NoError(t, err)
	_, err = db.Query(`insert into teams (name) values ('jack') returning id, name`)
	assert.NoError(t, err)
	_, err = db.Query(`insert into users (email) values ('jack') returning id, email`)
	assert.NoError(t, err)

	err = migrate.UpBy(log, db, fs, tableName, 1)
	assert.NoError(t, err)
	assert.NoError(t, err)
	_, err = db.Query(`insert into teams (name) values ('jack') returning id, name`)
	assert.NoError(t, err)
	_, err = db.Query(`insert into users (email) values ('jack') returning id, email`)
	assert.NoError(t, err)

	err = migrate.DownBy(log, db, fs, tableName, 1)
	assert.NoError(t, err)
	_, err = db.Query(`insert into teams (name) values ('jack') returning id, name`)
	assert.NoError(t, err)
	_, err = db.Query(`insert into users (email) values ('jack') returning id, email`)
	assert.Contains(t, err.Error(), "users")
	assert.Contains(t, err.Error(), "does not exist")

	err = migrate.DownBy(log, db, fs, tableName, 1)
	assert.NoError(t, err)
	_, err = db.Query(`insert into teams (name) values ('jack') returning id, name`)
	assert.Contains(t, err.Error(), "teams")
	assert.Contains(t, err.Error(), "does not exist")
	_, err = db.Query(`insert into users (email) values ('jack') returning id, email`)
	assert.Contains(t, err.Error(), "users")
	assert.Contains(t, err.Error(), "does not exist")

	err = migrate.DownBy(log, db, fs, tableName, 1)
	assert.NoError(t, err)
	_, err = db.Query(`insert into teams (name) values ('jack') returning id, name`)
	assert.Contains(t, err.Error(), "teams")
	assert.Contains(t, err.Error(), "does not exist")
	_, err = db.Query(`insert into users (email) values ('jack') returning id, email`)
	assert.Contains(t, err.Error(), "users")
	assert.Contains(t, err.Error(), "does not exist")
}

func TestUpRollback(t *testing.T) {
	drop(t)

	log := log.Log
	fs := httpfs.New(mapfs.New(map[string]string{
		"001_init.up.sql": `
			create extension if not exists citext;

			create table if not exists teams (
				id serial primary key not null,
				name citext not null
			);
		`,
		"001_init.down.sql": `
			drop table if exists teams;
			drop extension if exists citext;
		`,
		"002_users.up.sql": `
			create table if not exists users (
				id serial primary key not null
				email citext not null
			);
		`,
		"002_users.down.sql": `
			drop table if exists users;
		`,
	}))

	db, close := connect(t)
	defer close()

	err := migrate.UpBy(log, db, fs, tableName, 1)
	assert.NoError(t, err)

	_, err = db.Query(`insert into teams (name) values ('jack') returning id, name`)
	assert.NoError(t, err)
	_, err = db.Query(`insert into users (email) values ('jack') returning id, email`)
	assert.Contains(t, err.Error(), "users")
	assert.Contains(t, err.Error(), "does not exist")

	err = migrate.UpBy(log, db, fs, tableName, 1)
	assert.Equal(t, `002_users.up.sql failed. syntax error at or near "email" on column 2 in line 3:  (details: pq: syntax error at or near "email")`, err.Error())
	_, err = db.Query(`insert into teams (name) values ('jack') returning id, name`)
	assert.NoError(t, err)
	_, err = db.Query(`insert into users (email) values ('jack') returning id, email`)
	assert.Contains(t, err.Error(), "users")
	assert.Contains(t, err.Error(), "does not exist")
}

func TestDownRollback(t *testing.T) {
	drop(t)

	log := log.Log
	fs := httpfs.New(mapfs.New(map[string]string{
		"001_init.up.sql": `
			create extension if not exists citext;

			create table if not exists teams (
				id serial primary key not null,
				name citext not null
			);
		`,
		"001_init.down.sql": `
			drop table if exists teams;
			drop extension citex;
		`,
		"002_users.up.sql": `
			create table if not exists users (
				id serial primary key not null,
				email citext not null
			);
		`,
		"002_users.down.sql": `
			drop table if exists users;
		`,
	}))

	db, close := connect(t)
	defer close()

	// setup
	err := migrate.Up(log, db, fs, tableName)
	assert.NoError(t, err)

	err = migrate.DownBy(log, db, fs, tableName, 1)
	assert.NoError(t, err)
	_, err = db.Query(`insert into teams (name) values ('jack') returning id, name`)
	assert.NoError(t, err)
	_, err = db.Query(`insert into users (email) values ('jack') returning id, email`)
	assert.Contains(t, err.Error(), "users")
	assert.Contains(t, err.Error(), "does not exist")

	err = migrate.DownBy(log, db, fs, tableName, 1)
	assert.Equal(t, `001_init.down.sql failed. extension "citex" does not exist in line 0:  (details: pq: extension "citex" does not exist)`, err.Error())

	_, err = db.Query(`insert into teams (name) values ('jack') returning id, name`)
	assert.NoError(t, err)
}

func TestNew(t *testing.T) {
	log := log.Log

	// cleanup
	assert.NoError(t, os.RemoveAll("migrate"))

	err := migrate.New(log, "migrate", "setup")
	assert.NoError(t, err)
	exists(t, "migrate/001_setup.up.sql")
	exists(t, "migrate/001_setup.down.sql")

	err = migrate.New(log, "migrate", "create teams")
	assert.NoError(t, err)
	exists(t, "migrate/002_create_teams.up.sql")
	exists(t, "migrate/002_create_teams.down.sql")

	err = migrate.New(log, "migrate", "new-users")
	assert.NoError(t, err)
	exists(t, "migrate/003_new_users.up.sql")
	exists(t, "migrate/003_new_users.down.sql")

	if !t.Failed() {
		assert.NoError(t, os.RemoveAll("migrate"))
	}
}

func TestRemoteVersion(t *testing.T) {
	drop(t)

	log := log.Log
	fs := httpfs.New(mapfs.New(map[string]string{
		"001_init.up.sql": `
			create extension if not exists citext;

			create table if not exists teams (
				id serial primary key not null,
				name citext not null
			);
		`,
		"001_init.down.sql": `
			drop table if exists teams;
			drop extension citext;
		`,
		"002_users.up.sql": `
			create table if not exists users (
				id serial primary key not null,
				email citext not null
			);
		`,
		"002_users.down.sql": `
			drop table if exists users;
		`,
	}))

	db, close := connect(t)
	defer close()

	// setup
	err := migrate.Up(log, db, fs, tableName)
	assert.NoError(t, err)

	name, err := migrate.RemoteVersion(db, fs, tableName)
	assert.NoError(t, err)
	assert.Equal(t, `002_users.up.sql`, name)

	// teardown
	err = migrate.Down(log, db, fs, tableName)
	assert.NoError(t, err)

	name, err = migrate.RemoteVersion(db, fs, tableName)
	assert.Equal(t, migrate.ErrNoMigrations, err)
}

func TestLocalVersion(t *testing.T) {
	fs := httpfs.New(mapfs.New(map[string]string{
		"001_init.up.sql": `
			create extension if not exists citext;

			create table if not exists teams (
				id serial primary key not null,
				name citext not null
			);
		`,
		"001_init.down.sql": `
			drop table if exists teams;
			drop extension citext;
		`,
		"002_users.up.sql": `
			create table if not exists users (
				id serial primary key not null,
				email citext not null
			);
		`,
		"002_users.down.sql": `
			drop table if exists users;
		`,
	}))

	name, err := migrate.LocalVersion(fs)
	assert.NoError(t, err)
	assert.Equal(t, `002_users.up.sql`, name)
}
