package pgxmigrate_test

import (
	"context"
	"testing"
	"testing/fstest"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/matryer/is"
	"github.com/matthewmueller/migrate"
	"github.com/matthewmueller/migrate/pgxmigrate"
)

const (
	migrationTable = "pgxmigrate_integration_versions"
	projectsTable  = "pgxmigrate_integration_projects"
	membersTable   = "pgxmigrate_integration_members"
)

func TestMigrate(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, "postgres://localhost:5432/migrate-test?sslmode=disable")
	is.NoErr(err)
	t.Cleanup(pool.Close)

	dropTables(t, ctx, pool)
	t.Cleanup(func() {
		dropTables(t, ctx, pool)
	})

	fsys := fstest.MapFS{
		"001_projects.up.sql": {
			Data: []byte("create table " + projectsTable + " (id bigint primary key);"),
		},
		"001_projects.down.sql": {
			Data: []byte("drop table " + projectsTable + ";"),
		},
		"002_members.up.sql": {
			Data: []byte("create table " + membersTable + " (id bigint primary key);"),
		},
		"002_members.down.sql": {
			Data: []byte("drop table " + membersTable + ";"),
		},
	}

	is.NoErr(pgxmigrate.UpBy(nil, pool, fsys, migrationTable, 1))
	assertPoolUsable(t, ctx, pool)

	version, err := pgxmigrate.RemoteVersion(pool, fsys, migrationTable)
	is.NoErr(err)
	is.Equal("001_projects.up.sql", version)
	assertPoolUsable(t, ctx, pool)

	is.NoErr(pgxmigrate.Up(nil, pool, fsys, migrationTable))
	assertPoolUsable(t, ctx, pool)

	version, err = pgxmigrate.RemoteVersion(pool, fsys, migrationTable)
	is.NoErr(err)
	is.Equal("002_members.up.sql", version)
	assertPoolUsable(t, ctx, pool)

	_, err = pool.Exec(ctx, "insert into "+membersTable+" (id) values (1)")
	is.NoErr(err)
	is.NoErr(pgxmigrate.Redo(nil, pool, fsys, migrationTable))
	assertPoolUsable(t, ctx, pool)

	var members int
	err = pool.QueryRow(ctx, "select count(*) from "+membersTable).Scan(&members)
	is.NoErr(err)
	is.Equal(0, members)

	is.NoErr(pgxmigrate.DownBy(nil, pool, fsys, migrationTable, 1))
	assertPoolUsable(t, ctx, pool)

	version, err = pgxmigrate.RemoteVersion(pool, fsys, migrationTable)
	is.NoErr(err)
	is.Equal("001_projects.up.sql", version)
	assertPoolUsable(t, ctx, pool)

	is.NoErr(pgxmigrate.Down(nil, pool, fsys, migrationTable))
	assertPoolUsable(t, ctx, pool)

	_, err = pgxmigrate.RemoteVersion(pool, fsys, migrationTable)
	is.Equal(migrate.ErrNoMigrations, err)
	assertPoolUsable(t, ctx, pool)
}

func assertPoolUsable(t testing.TB, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	is := is.New(t)
	is.NoErr(pool.Ping(ctx))
}

func dropTables(t testing.TB, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	is := is.New(t)
	_, err := pool.Exec(ctx, `
		drop table if exists pgxmigrate_integration_members;
		drop table if exists pgxmigrate_integration_projects;
		drop table if exists pgxmigrate_integration_versions;
	`)
	is.NoErr(err)
}
