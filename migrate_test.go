package migrate_test

import (
	"database/sql"
	"io/ioutil"
	"path"
	"sort"
	"testing"

	"github.com/matthewmueller/migrate"

	"github.com/stretchr/testify/assert"
)

func connect(t testing.TB) (*sql.DB, func()) {
	db, err := sql.Open("postgres", "postgres://localhost:5432/migrate?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}

	if err := db.Ping(); err != nil {
		t.Fatal(err)
	}

	return db, func() {
		if err := db.Close(); err != nil {
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
		if err := migrate.Create(dir, name); err != nil {
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

func drop(t testing.TB, db *sql.DB) {
	if _, err := db.Exec(`drop table if exists "users"; drop table if exists "schema_migrations";`); err != nil {
		t.Fatal(err)
	}
}

func version(t testing.TB, db *sql.DB, expected int) {
	v, err := migrate.Version(db)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, uint(expected), v)
}

func emails(t testing.TB, db *sql.DB) (emails []string) {
	rows, err := db.Query(`select * from "users" order by email asc;`)
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

	if err := migrate.Up(db, dir, nil); err != nil {
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

	var n uint = 2
	if err := migrate.Up(db, dir, &n); err != nil {
		t.Fatal(err)
	}
	defer drop(t, db)

	es := emails(t, db)
	assert.Len(t, es, 1)
	assert.Equal(t, "m@gmail.com", es[0])
	version(t, db, 2)

	if err := migrate.Up(db, dir, nil); err != nil {
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

	var n uint = 2
	if err := migrate.Up(db, dir, &n); err != nil {
		t.Fatal(err)
	}
	defer drop(t, db)

	es := emails(t, db)
	assert.Len(t, es, 1)
	assert.Equal(t, "m@gmail.com", es[0])
	version(t, db, 2)

	if err := migrate.Up(db, dir, nil); err != nil {
		t.Fatal(err)
	}
	es = emails(t, db)
	assert.Len(t, es, 2)
	assert.Equal(t, "m@gmail.com", es[0])
	assert.Equal(t, "t@gmail.com", es[1])
	version(t, db, 3)

	if err := migrate.Up(db, dir, nil); err != nil {
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

	if err := migrate.Up(db, dir, nil); err != nil {
		t.Fatal(err)
	}
	defer drop(t, db)

	es := emails(t, db)
	assert.Len(t, es, 2)
	assert.Equal(t, "m@gmail.com", es[0])
	assert.Equal(t, "t@gmail.com", es[1])
	version(t, db, 3)

	var n uint = 2
	if err := migrate.Down(db, dir, &n); err != nil {
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

	if err := migrate.Up(db, dir, nil); err != nil {
		t.Fatal(err)
	}
	defer drop(t, db)

	es := emails(t, db)
	assert.Len(t, es, 2)
	assert.Equal(t, "m@gmail.com", es[0])
	assert.Equal(t, "t@gmail.com", es[1])
	version(t, db, 3)

	if err := migrate.Down(db, dir, nil); err != nil {
		t.Fatal(err)
	}

	_, err := db.Query(`select * from "users" order by email asc;`)
	assert.EqualError(t, err, "pq: relation \"users\" does not exist")
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

	if err := migrate.Up(db, dir, nil); err != nil {
		t.Fatal(err)
	}
	defer drop(t, db)

	es := emails(t, db)
	assert.Len(t, es, 2)
	assert.Equal(t, "m@gmail.com", es[0])
	assert.Equal(t, "t@gmail.com", es[1])
	version(t, db, 3)

	if err := migrate.Down(db, dir, nil); err != nil {
		t.Fatal(err)
	}

	_, err := db.Query(`select * from "users" order by email asc;`)
	assert.EqualError(t, err, "pq: relation \"users\" does not exist")
	version(t, db, 0)

	if err := migrate.Up(db, dir, nil); err != nil {
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

	err := migrate.Up(db, dir, nil)
	version(t, db, 0)
	assert.EqualError(t, err, "003_three.up.sql: syntax error at or near \"email\" (line: 1)")
}
