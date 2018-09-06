package migrate_test

import (
	"io/ioutil"
	"path"
	"sort"
	"testing"

	"github.com/jackc/pgx"
	"github.com/matthewmueller/migrate"

	"github.com/stretchr/testify/assert"
)

var url = "postgres://localhost:5432/migrate?sslmode=disable"

func connect(t testing.TB) (*pgx.Conn, func()) {
	cfg, err := pgx.ParseConnectionString(url)
	if err != nil {
		t.Fatal(err)
	}

	conn, err := pgx.Connect(cfg)
	if err != nil {
		t.Fatal(err)
	}

	return conn, func() {
		if err := conn.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func create(t testing.TB, names ...string) (dir string) {
	dir, err := ioutil.TempDir("", "migrate_")
	if err != nil {
		t.Fatal(err)
	}

	for _, name := range names {
		if err := migrate.New(dir, name); err != nil {
			t.Fatal(err)
		}
	}

	return dir
}

func write(t testing.TB, name, data string) {
	if err := ioutil.WriteFile(name, []byte(data), 0644); err != nil {
		t.Fatal(err)
	}
}

func drop(t testing.TB, conn *pgx.Conn) {
	if _, err := conn.Exec(`drop table if exists "users"; drop table if exists "schema_migrations";`); err != nil {
		t.Fatal(err)
	}
}

func version(t testing.TB, conn *pgx.Conn, expected int) {
	v, err := migrate.Version(conn)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, uint(expected), v)
}

func emails(t testing.TB, conn *pgx.Conn) (emails []string) {
	rows, err := conn.Query(`select * from "users" order by email asc;`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var email string
		if err := rows.Scan(&email); err != nil {
			t.Fatal(err)
		}
		emails = append(emails, email)
	}

	return emails
}

func TestConnect(t *testing.T) {
	db, close := connect(t)
	defer close()
	assert.NotNil(t, db)
}

func TestCreate(t *testing.T) {
	dir := create(t, "one", "two", "three")
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}

	var names []string
	for _, file := range files {
		names = append(names, file.Name())
	}
	sort.Strings(names)

	assert.Len(t, names, 6)
	assert.Equal(t, "001_one.down.sql", names[0])
	assert.Equal(t, "001_one.up.sql", names[1])
	assert.Equal(t, "002_two.down.sql", names[2])
	assert.Equal(t, "002_two.up.sql", names[3])
	assert.Equal(t, "003_three.down.sql", names[4])
	assert.Equal(t, "003_three.up.sql", names[5])
}

func TestMigrateUp(t *testing.T) {
	dir := create(t, "one", "two", "three")
	write(t, path.Join(dir, "001_one.up.sql"), `create table if not exists "users" (email text not null primary key);`)
	write(t, path.Join(dir, "002_two.up.sql"), `insert into "users" (email) values ('m@gmail.com');`)
	write(t, path.Join(dir, "003_three.up.sql"), `insert into "users" (email) values ('t@gmail.com');`)

	db, close := connect(t)
	defer close()

	if err := migrate.Up(db, dir); err != nil {
		t.Fatal(err)
	}
	defer drop(t, db)

	es := emails(t, db)
	assert.Len(t, es, 2)
	assert.Equal(t, "m@gmail.com", es[0])
	assert.Equal(t, "t@gmail.com", es[1])

	version(t, db, 3)
}

func TestMigrateUpTwice(t *testing.T) {
	dir := create(t, "one", "two", "three")
	write(t, path.Join(dir, "001_one.up.sql"), `create table if not exists "users" (email text not null primary key);`)
	write(t, path.Join(dir, "002_two.up.sql"), `insert into "users" (email) values ('m@gmail.com');`)
	write(t, path.Join(dir, "003_three.up.sql"), `insert into "users" (email) values ('t@gmail.com');`)

	db, close := connect(t)
	defer close()

	if err := migrate.UpTo(db, dir, "two"); err != nil {
		t.Fatal(err)
	}
	defer drop(t, db)

	es := emails(t, db)
	assert.Len(t, es, 1)
	assert.Equal(t, "m@gmail.com", es[0])
	version(t, db, 2)

	if err := migrate.Up(db, dir); err != nil {
		t.Fatal(err)
	}
	es = emails(t, db)
	assert.Len(t, es, 2)
	assert.Equal(t, "m@gmail.com", es[0])
	assert.Equal(t, "t@gmail.com", es[1])
	version(t, db, 3)
}

func TestMigrateUpThrice(t *testing.T) {
	dir := create(t, "one", "two", "three")
	write(t, path.Join(dir, "001_one.up.sql"), `create table if not exists "users" (email text not null primary key);`)
	write(t, path.Join(dir, "002_two.up.sql"), `insert into "users" (email) values ('m@gmail.com');`)
	write(t, path.Join(dir, "003_three.up.sql"), `insert into "users" (email) values ('t@gmail.com');`)

	db, close := connect(t)
	defer close()

	if err := migrate.UpTo(db, dir, "two"); err != nil {
		t.Fatal(err)
	}
	defer drop(t, db)

	es := emails(t, db)
	assert.Len(t, es, 1)
	assert.Equal(t, "m@gmail.com", es[0])
	version(t, db, 2)

	if err := migrate.Up(db, dir); err != nil {
		t.Fatal(err)
	}
	es = emails(t, db)
	assert.Len(t, es, 2)
	assert.Equal(t, "m@gmail.com", es[0])
	assert.Equal(t, "t@gmail.com", es[1])
	version(t, db, 3)

	if err := migrate.Up(db, dir); err != nil {
		t.Fatal(err)
	}
	es = emails(t, db)
	assert.Len(t, es, 2)
	assert.Equal(t, "m@gmail.com", es[0])
	assert.Equal(t, "t@gmail.com", es[1])
	version(t, db, 3)
}

func TestMigrateUpDownTwo(t *testing.T) {
	dir := create(t, "one", "two", "three")
	write(t, path.Join(dir, "001_one.up.sql"), `create table if not exists "users" (email text not null primary key);`)
	write(t, path.Join(dir, "001_one.down.sql"), `drop table if exists "users";`)
	write(t, path.Join(dir, "002_two.up.sql"), `insert into "users" (email) values ('m@gmail.com');`)
	write(t, path.Join(dir, "002_two.down.sql"), `delete from "users" where email='m@gmail.com';`)
	write(t, path.Join(dir, "003_three.up.sql"), `insert into "users" (email) values ('t@gmail.com');`)
	write(t, path.Join(dir, "003_three.down.sql"), `delete from "users" where email='t@gmail.com';`)

	db, close := connect(t)
	defer close()

	if err := migrate.Up(db, dir); err != nil {
		t.Fatal(err)
	}
	defer drop(t, db)

	es := emails(t, db)
	assert.Len(t, es, 2)
	assert.Equal(t, "m@gmail.com", es[0])
	assert.Equal(t, "t@gmail.com", es[1])
	version(t, db, 3)

	if err := migrate.DownTo(db, dir, "two"); err != nil {
		t.Fatal(err)
	}

	es = emails(t, db)
	assert.Len(t, es, 0)
	version(t, db, 1)
}

func TestMigrateUpDown(t *testing.T) {
	dir := create(t, "one", "two", "three")
	write(t, path.Join(dir, "001_one.up.sql"), `create table if not exists "users" (email text not null primary key);`)
	write(t, path.Join(dir, "001_one.down.sql"), `drop table if exists "users";`)
	write(t, path.Join(dir, "002_two.up.sql"), `insert into "users" (email) values ('m@gmail.com');`)
	write(t, path.Join(dir, "002_two.down.sql"), `delete from "users" where email='m@gmail.com';`)
	write(t, path.Join(dir, "003_three.up.sql"), `insert into "users" (email) values ('t@gmail.com');`)
	write(t, path.Join(dir, "003_three.down.sql"), `delete from "users" where email='t@gmail.com';`)

	db, close := connect(t)
	defer close()

	if err := migrate.Up(db, dir); err != nil {
		t.Fatal(err)
	}
	defer drop(t, db)

	es := emails(t, db)
	assert.Len(t, es, 2)
	assert.Equal(t, "m@gmail.com", es[0])
	assert.Equal(t, "t@gmail.com", es[1])
	version(t, db, 3)

	if err := migrate.Down(db, dir); err != nil {
		t.Fatal(err)
	}

	_, err := db.Query(`select * from "users" order by email asc;`)
	assert.EqualError(t, err, "ERROR: relation \"users\" does not exist (SQLSTATE 42P01)")
	version(t, db, 0)
}

func TestMigrateUpDownUp(t *testing.T) {
	dir := create(t, "one", "two", "three")
	write(t, path.Join(dir, "001_one.up.sql"), `create table if not exists "users" (email text not null primary key);`)
	write(t, path.Join(dir, "001_one.down.sql"), `drop table if exists "users";`)
	write(t, path.Join(dir, "002_two.up.sql"), `insert into "users" (email) values ('m@gmail.com');`)
	write(t, path.Join(dir, "002_two.down.sql"), `delete from "users" where email='m@gmail.com';`)
	write(t, path.Join(dir, "003_three.up.sql"), `insert into "users" (email) values ('t@gmail.com');`)
	write(t, path.Join(dir, "003_three.down.sql"), `delete from "users" where email='t@gmail.com';`)

	db, close := connect(t)
	defer close()

	if err := migrate.Up(db, dir); err != nil {
		t.Fatal(err)
	}
	defer drop(t, db)

	es := emails(t, db)
	assert.Len(t, es, 2)
	assert.Equal(t, "m@gmail.com", es[0])
	assert.Equal(t, "t@gmail.com", es[1])
	version(t, db, 3)

	if err := migrate.Down(db, dir); err != nil {
		t.Fatal(err)
	}

	_, err := db.Query(`select * from "users" order by email asc;`)
	assert.EqualError(t, err, "ERROR: relation \"users\" does not exist (SQLSTATE 42P01)")
	version(t, db, 0)

	if err := migrate.Up(db, dir); err != nil {
		t.Fatal(err)
	}

	es = emails(t, db)
	assert.Len(t, es, 2)
	assert.Equal(t, "m@gmail.com", es[0])
	assert.Equal(t, "t@gmail.com", es[1])
	version(t, db, 3)
}

func TestMigrateUpSyntaxError(t *testing.T) {
	dir := create(t, "one", "two", "three")
	write(t, path.Join(dir, "001_one.up.sql"), `create table if not exists "users" (email text not null primary key);`)
	write(t, path.Join(dir, "002_two.up.sql"), `insert into "users" (email) values ('m@gmail.com');`)
	write(t, path.Join(dir, "003_three.up.sql"), `insert into "users" email) values ('t@gmail.com');`)

	db, close := connect(t)
	defer close()

	err := migrate.Up(db, dir)
	version(t, db, 0)
	assert.EqualError(t, err, "003_three.up.sql:1 syntax error at or near \"email\"")
}
