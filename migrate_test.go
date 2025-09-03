package migrate_test

import (
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/matryer/is"
	"github.com/matthewmueller/migrate"
	"github.com/matthewmueller/virt"
	"github.com/xo/dburl"

	// sqlite db
	_ "github.com/jackc/pgx/v5/stdlib"
	_ "github.com/mattn/go-sqlite3"
)

const tableName = "migrate"

func TestPostgres(t *testing.T) {
	url := "postgres://localhost:5432/migrate-test?sslmode=disable"
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.fn(t, url)
		})
	}
}

func TestSQLite(t *testing.T) {
	url := "sqlite://tmp.db"
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			is := is.New(t)
			is.NoErr(os.RemoveAll("./tmp.db"))
			test.fn(t, url)
		})
	}
}

func dial(connString string) (*sql.DB, error) {
	url, err := dburl.Parse(connString)
	if err != nil {
		return nil, err
	}
	switch url.Scheme {
	case "postgres":
		return sql.Open("pgx", url.DSN)
	case "sqlite", "sqlite3":
		return sql.Open("sqlite3", url.DSN)
	default:
		return nil, fmt.Errorf("migrate doesn't support this url scheme: %s", url.Scheme)
	}
}

func connect(t testing.TB, url string) (*sql.DB, func()) {
	t.Helper()
	is := is.New(t)
	db, err := dial(url)
	is.NoErr(err)
	return db, func() {
		is.NoErr(db.Close())
	}
}

func drop(t testing.TB, url string) {
	t.Helper()
	is := is.New(t)
	db, close := connect(t, url)
	defer close()
	_, err := db.Exec(`
		drop table if exists migrate;
		drop table if exists users;
		drop table if exists teams;
	`)
	is.NoErr(err)
}

func exists(t testing.TB, path string) {
	t.Helper()
	is := is.New(t)
	_, err := os.Stat(path)
	is.NoErr(err)
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
		name: "no migrations",
		fn: func(t testing.TB, url string) {
			drop(t, url)
			is := is.New(t)
			fs := fstest.MapFS{}
			db, close := connect(t, url)
			defer close()
			err := migrate.Up(nil, db, fs, tableName)
			is.Equal(migrate.ErrNoMigrations, err)
		},
	},
	{
		name: "no matching migrations",
		fn: func(t testing.TB, url string) {
			drop(t, url)
			is := is.New(t)
			fs := fstest.MapFS{
				"migrate/001_init.up.sql":   {Data: []byte(``)},
				"migrate/001_init.down.sql": {Data: []byte(``)},
			}
			db, close := connect(t, url)
			defer close()
			err := migrate.Up(nil, db, fs, tableName)
			is.Equal(migrate.ErrNoMigrations, err)
		},
	},
	{
		name: "no migrations down",
		fn: func(t testing.TB, url string) {
			drop(t, url)
			is := is.New(t)
			fs := fstest.MapFS{}
			db, close := connect(t, url)
			defer close()
			err := migrate.Down(nil, db, fs, tableName)
			is.Equal(migrate.ErrNoMigrations, err)
		},
	},
	{
		name: "no matching migrations down",
		fn: func(t testing.TB, url string) {
			drop(t, url)
			is := is.New(t)
			fs := fstest.MapFS{
				"migrate/001_init.up.sql":   {Data: []byte(``)},
				"migrate/001_init.down.sql": {Data: []byte(``)},
			}
			db, close := connect(t, url)
			defer close()
			err := migrate.Down(nil, db, fs, tableName)
			is.Equal(migrate.ErrNoMigrations, err)
		},
	},
	{
		name: "zeroth",
		fn: func(t testing.TB, url string) {
			drop(t, url)
			is := is.New(t)
			fs := fstest.MapFS{
				"000_init.up.sql": {
					Data: []byte(`
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
					`),
				},
				"000_init.down.sql": {
					Data: []byte(`
						drop table if exists users;
						drop table if exists teams;
					`),
				},
			}
			db, close := connect(t, url)
			defer close()
			err := migrate.Up(nil, db, fs, tableName)
			is.Equal(migrate.ErrZerothMigration, err)
		},
	},
	{
		name: "up down",
		fn: func(t testing.TB, url string) {
			drop(t, url)
			is := is.New(t)

			fs := fstest.MapFS{
				"001_init.up.sql": {
					Data: []byte(`
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
					`),
				},
				"001_init.down.sql": {
					Data: []byte(`
						drop table if exists users;
						drop table if exists teams;
					`),
				},
			}

			db, close := connect(t, url)
			defer close()

			err := migrate.Up(nil, db, fs, tableName)
			is.NoErr(err)

			rows, err := db.Query(`insert into teams (id, name) values (1, 'jack') returning *`)
			is.NoErr(err)
			for rows.Next() {
				var id int
				var name string
				err := rows.Scan(&id, &name)
				is.NoErr(err)
				is.Equal(1, id)
				is.Equal("jack", name)
			}
			is.NoErr(rows.Err())

			err = migrate.Down(nil, db, fs, tableName)
			is.NoErr(err)

			_, err = db.Query(`insert into teams (id, name) values (2, 'jack') returning *`)
			is.True(err != nil)
			is.True(strings.Contains(err.Error(), "teams"))
			is.True(notExists(err, "teams"))
		},
	},
	{
		name: "up down no logger",
		fn: func(t testing.TB, url string) {
			drop(t, url)
			is := is.New(t)

			fs := fstest.MapFS{
				"001_init.up.sql": {
					Data: []byte(`
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
					`),
				},
				"001_init.down.sql": {
					Data: []byte(`
						drop table if exists users;
						drop table if exists teams;
					`),
				},
			}

			db, close := connect(t, url)
			defer close()

			err := migrate.Up(nil, db, fs, tableName)
			is.NoErr(err)

			rows, err := db.Query(`insert into teams (id, name) values (1, 'jack') returning *`)
			is.NoErr(err)
			for rows.Next() {
				var id int
				var name string
				err := rows.Scan(&id, &name)
				is.NoErr(err)
				is.Equal(1, id)
				is.Equal("jack", name)
			}
			is.NoErr(rows.Err())

			err = migrate.Down(nil, db, fs, tableName)
			is.NoErr(err)

			_, err = db.Query(`insert into teams (id, name) values (2, 'jack') returning *`)
			is.True(err != nil)
			is.True(strings.Contains(err.Error(), "teams"))
			is.True(notExists(err, "teams"))
		},
	},
	{
		name: "upupdowndown",
		fn: func(t testing.TB, url string) {
			drop(t, url)
			is := is.New(t)

			fs := fstest.MapFS{
				"001_init.up.sql": {
					Data: []byte(`
						create table if not exists teams (
							id serial primary key not null,
							name text not null
						);
					`),
				},
				"001_init.down.sql": {
					Data: []byte(`
						drop table if exists teams;
					`),
				},
				"002_users.up.sql": {
					Data: []byte(`
						create table if not exists users (
							id serial primary key not null,
							email text not null
						);
					`),
				},
				"002_users.down.sql": {
					Data: []byte(`
						drop table if exists users;
					`),
				},
			}

			db, close := connect(t, url)
			defer close()

			err := migrate.Up(nil, db, fs, tableName)
			is.NoErr(err)
			err = migrate.Up(nil, db, fs, tableName)
			is.NoErr(err)

			rows, err := db.Query(`insert into users (id, email) values (1, 'jack') returning *`)
			is.NoErr(err)
			for rows.Next() {
				var id int
				var email string
				err := rows.Scan(&id, &email)
				is.NoErr(err)
				is.Equal(1, id)
				is.Equal("jack", email)
			}
			is.NoErr(rows.Err())

			err = migrate.Down(nil, db, fs, tableName)
			is.NoErr(err)
			err = migrate.Down(nil, db, fs, tableName)
			is.NoErr(err)

			_, err = db.Query(`insert into users (id, email) values (2, 'jack') returning *`)
			is.True(err != nil)
			is.True(strings.Contains(err.Error(), "users"))
			is.True(notExists(err, "users"))
		},
	},
	{
		name: "upbydownby",
		fn: func(t testing.TB, url string) {
			drop(t, url)
			is := is.New(t)

			fs := fstest.MapFS{
				"001_init.up.sql": {
					Data: []byte(`
						create table if not exists teams (
							id serial primary key not null,
							name text not null
						);
					`),
				},
				"001_init.down.sql": {
					Data: []byte(`
						drop table if exists teams;
					`),
				},
				"002_users.up.sql": {
					Data: []byte(`
						create table if not exists users (
							id serial primary key not null,
							email text not null
						);
					`),
				},
				"002_users.down.sql": {
					Data: []byte(`
						drop table if exists users;
					`),
				},
			}

			db, close := connect(t, url)
			defer close()

			err := migrate.UpBy(nil, db, fs, tableName, 1)
			is.NoErr(err)

			_, err = db.Query(`insert into teams (name) values ('jack') returning *`)
			is.NoErr(err)
			_, err = db.Query(`insert into users (email) values ('jack') returning *`)
			is.True(err != nil)
			is.True(strings.Contains(err.Error(), "users"))
			is.True(notExists(err, "users"))

			err = migrate.UpBy(nil, db, fs, tableName, 1)
			is.NoErr(err)
			_, err = db.Query(`insert into teams (name) values ('jack') returning *`)
			is.NoErr(err)
			_, err = db.Query(`insert into users (email) values ('jack') returning *`)
			is.NoErr(err)

			err = migrate.UpBy(nil, db, fs, tableName, 1)
			is.NoErr(err)
			is.NoErr(err)
			_, err = db.Query(`insert into teams (name) values ('jack') returning *`)
			is.NoErr(err)
			_, err = db.Query(`insert into users (email) values ('jack') returning *`)
			is.NoErr(err)

			err = migrate.DownBy(nil, db, fs, tableName, 1)
			is.NoErr(err)
			_, err = db.Query(`insert into teams (name) values ('jack') returning *`)
			is.NoErr(err)
			_, err = db.Query(`insert into users (email) values ('jack') returning *`)
			is.True(err != nil)
			is.True(strings.Contains(err.Error(), "users"))
			is.True(notExists(err, "users"))

			err = migrate.DownBy(nil, db, fs, tableName, 1)
			is.NoErr(err)
			_, err = db.Query(`insert into teams (name) values ('jack') returning *`)
			is.True(strings.Contains(err.Error(), "teams"))
			is.True(notExists(err, "teams"))
			_, err = db.Query(`insert into users (email) values ('jack') returning *`)
			is.True(strings.Contains(err.Error(), "users"))
			is.True(notExists(err, "users"))

			err = migrate.DownBy(nil, db, fs, tableName, 1)
			is.NoErr(err)
			_, err = db.Query(`insert into teams (name) values ('jack') returning *`)
			is.True(strings.Contains(err.Error(), "teams"))
			is.True(notExists(err, "teams"))
			_, err = db.Query(`insert into users (email) values ('jack') returning *`)
			is.True(strings.Contains(err.Error(), "users"))
			is.True(notExists(err, "users"))
		},
	},
	{
		name: "uprollback",
		fn: func(t testing.TB, url string) {
			drop(t, url)
			is := is.New(t)

			fs := fstest.MapFS{
				"001_init.up.sql": {
					Data: []byte(`
						create table if not exists teams (
							id serial primary key not null,
							name text not null
						);
					`),
				},
				"001_init.down.sql": {
					Data: []byte(`
						drop table if exists teams;
					`),
				},
				"002_users.up.sql": {
					Data: []byte(`
						create table if not exists users (
							id serial primary key not null -- intentionally missing comma
							email text not null
						);
					`),
				},
				"002_users.down.sql": {
					Data: []byte(`
						drop table if exists users;
					`),
				},
			}

			db, close := connect(t, url)
			defer close()

			err := migrate.UpBy(nil, db, fs, tableName, 1)
			is.NoErr(err)

			_, err = db.Query(`insert into teams (name) values ('jack') returning *`)
			is.NoErr(err)
			_, err = db.Query(`insert into users (email) values ('jack') returning *`)
			is.True(err != nil)
			is.True(strings.Contains(err.Error(), "users"))
			is.True(notExists(err, "users"))

			err = migrate.UpBy(nil, db, fs, tableName, 1)
			is.True(err != nil)
			is.True(syntaxError(err, "email"))

			_, err = db.Query(`insert into teams (name) values ('jack') returning *`)
			is.NoErr(err)
			_, err = db.Query(`insert into users (email) values ('jack') returning *`)
			is.True(strings.Contains(err.Error(), "users"))
			is.True(notExists(err, "users"))
		},
	},
	{
		name: "downrollback",
		fn: func(t testing.TB, url string) {
			drop(t, url)
			is := is.New(t)

			fs := fstest.MapFS{
				"001_init.up.sql": {
					Data: []byte(`
						create table if not exists teams (
							id serial primary key not null,
							name text not null
						);
					`),
				},
				"001_init.down.sql": {
					Data: []byte(`
						drop table if exis teams; -- intentional syntax error
					`),
				},
				"002_users.up.sql": {
					Data: []byte(`
						create table if not exists users (
							id serial primary key not null,
							email text not null
						);
					`),
				},
				"002_users.down.sql": {
					Data: []byte(`
						drop table if exists users;
					`),
				},
			}

			db, close := connect(t, url)
			defer close()

			// setup
			err := migrate.Up(nil, db, fs, tableName)
			is.NoErr(err)

			err = migrate.DownBy(nil, db, fs, tableName, 1)
			is.NoErr(err)
			_, err = db.Query(`insert into teams (name) values ('jack') returning *`)
			is.NoErr(err)
			_, err = db.Query(`insert into users (email) values ('jack') returning *`)
			is.True(err != nil)
			is.True(strings.Contains(err.Error(), "users"))
			is.True(notExists(err, "users"))

			err = migrate.DownBy(nil, db, fs, tableName, 1)
			is.True(err != nil)
			is.True(syntaxError(err, "exis"))

			_, err = db.Query(`insert into teams (name) values ('jack') returning *`)
			is.NoErr(err)
		},
	},
	{
		name: "new",
		fn: func(t testing.TB, url string) {
			drop(t, url)
			is := is.New(t)

			// cleanup
			is.NoErr(os.RemoveAll("migrate"))
			is.NoErr(os.MkdirAll("migrate", 0755))

			err := migrate.New(nil, virt.OS("migrate"), "setup")
			is.NoErr(err)
			exists(t, "migrate/001_setup.up.sql")
			exists(t, "migrate/001_setup.down.sql")

			err = migrate.New(nil, virt.OS("migrate"), "create teams")
			is.NoErr(err)
			exists(t, "migrate/002_create_teams.up.sql")
			exists(t, "migrate/002_create_teams.down.sql")

			err = migrate.New(nil, virt.OS("migrate"), "new-users")
			is.NoErr(err)
			exists(t, "migrate/003_new_users.up.sql")
			exists(t, "migrate/003_new_users.down.sql")

			if !t.Failed() {
				is.NoErr(os.RemoveAll("migrate"))
			}
		},
	},
	{
		name: "remoteversion",
		fn: func(t testing.TB, url string) {
			drop(t, url)
			is := is.New(t)

			fs := fstest.MapFS{
				"001_init.up.sql": {
					Data: []byte(`
						create table if not exists teams (
							id serial primary key not null,
							name text not null
						);
					`),
				},
				"001_init.down.sql": {
					Data: []byte(`
						drop table if exists teams;
					`),
				},
				"002_users.up.sql": {
					Data: []byte(`
						create table if not exists users (
							id serial primary key not null,
							email text not null
						);
					`),
				},
				"002_users.down.sql": {
					Data: []byte(`
						drop table if exists users;
					`),
				},
			}

			db, close := connect(t, url)
			defer close()

			// setup
			err := migrate.Up(nil, db, fs, tableName)
			is.NoErr(err)

			name, err := migrate.RemoteVersion(db, fs, tableName)
			is.NoErr(err)
			is.Equal(`002_users.up.sql`, name)

			// teardown
			err = migrate.Down(nil, db, fs, tableName)
			is.NoErr(err)

			_, err = migrate.RemoteVersion(db, fs, tableName)
			is.Equal(migrate.ErrNoMigrations, err)
		},
	},
	{
		name: "localversion",
		fn: func(t testing.TB, url string) {
			drop(t, url)
			is := is.New(t)

			fs := fstest.MapFS{
				"001_init.up.sql": {
					Data: []byte(`
						create table if not exists teams (
							id serial primary key not null,
							name text not null
						);
					`),
				},
				"001_init.down.sql": {
					Data: []byte(`
						drop table if exists teams;
					`),
				},
				"002_users.up.sql": {
					Data: []byte(`
						create table if not exists users (
							id serial primary key not null,
							email text not null
						);
					`),
				},
				"002_users.down.sql": {
					Data: []byte(`
						drop table if exists users;
					`),
				},
			}

			name, err := migrate.LocalVersion(fs)
			is.NoErr(err)
			is.Equal(`002_users.up.sql`, name)
		},
	},
	{
		name: "redo",
		fn: func(t testing.TB, url string) {
			drop(t, url)
			is := is.New(t)

			fs := fstest.MapFS{
				"001_init.up.sql": {
					Data: []byte(`
						create table if not exists teams (
							id serial primary key not null,
							name text not null
						);
					`),
				},
				"001_init.down.sql": {
					Data: []byte(`
						drop table if exists teams;
					`),
				},
				"002_users.up.sql": {
					Data: []byte(`
						create table if not exists users (
							id serial primary key not null,
							email text not null
						);
					`),
				},
				"002_users.down.sql": {
					Data: []byte(`
						drop table if exists users;
					`),
				},
			}

			db, close := connect(t, url)
			defer close()

			is.NoErr(migrate.Up(nil, db, fs, "migrate"))

			rows, err := db.Query(`insert into users (id, email) values (1, 'jack@standupjack.com') returning *`)
			is.NoErr(err)
			for rows.Next() {
				var id int
				var email string
				err := rows.Scan(&id, &email)
				is.NoErr(err)
				is.Equal(1, id)
				is.Equal("jack@standupjack.com", email)
			}
			is.NoErr(rows.Err())
			is.NoErr(rows.Close())

			err = migrate.Redo(nil, db, fs, "migrate")
			is.NoErr(err)

			rows, err = db.Query(`select * from users`)
			is.NoErr(err)
			count := 0
			for rows.Next() {
				count++
			}
			is.NoErr(rows.Err())
			is.NoErr(rows.Close())
			is.Equal(0, count)

			name, err := migrate.RemoteVersion(db, fs, tableName)
			is.NoErr(err)
			is.Equal(`002_users.up.sql`, name)
		},
	},
	{
		name: "redo failure rollback",
		fn: func(t testing.TB, url string) {
			drop(t, url)
			is := is.New(t)

			fs := fstest.MapFS{
				"001_init.up.sql": {
					Data: []byte(`
						create table if not exists teams (
							id serial primary key not null,
							name text not null
						);
					`),
				},
				"001_init.down.sql": {
					Data: []byte(`
						drop table if exists teams;
					`),
				},
				"002_users.up.sql": {
					Data: []byte(`
						create table if not exists users (
							id serial primary key not null,
							email text not null
						);
					`),
				},
				"002_users.down.sql": {
					Data: []byte(`
						drop table if exists users;
					`),
				},
			}

			db, close := connect(t, url)
			defer close()

			is.NoErr(migrate.Up(nil, db, fs, "migrate"))

			rows, err := db.Query(`insert into users (id, email) values (1, 'jack@standupjack.com') returning *`)
			is.NoErr(err)
			for rows.Next() {
				var id int
				var email string
				err := rows.Scan(&id, &email)
				is.NoErr(err)
				is.Equal(1, id)
				is.Equal("jack@standupjack.com", email)
			}
			is.NoErr(rows.Err())
			is.NoErr(rows.Close())

			// make the up migration fail
			fs["002_users.up.sql"] = &fstest.MapFile{
				Data: []byte(`
					create table if not exists users (
						id serial primary key not null,
						email text not null,
					; -- syntax error
				`),
			}

			err = migrate.Redo(nil, db, fs, "migrate")
			is.True(err != nil)

			rows, err = db.Query(`select * from users`)
			is.NoErr(err)
			count := 0
			for rows.Next() {
				count++
			}
			is.NoErr(rows.Err())
			is.NoErr(rows.Close())
			is.Equal(1, count)

			name, err := migrate.RemoteVersion(db, fs, tableName)
			is.NoErr(err)
			is.Equal(`002_users.up.sql`, name)
		},
	},
}
