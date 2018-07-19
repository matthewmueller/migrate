package migrate

import (
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"

	// postgres
	"github.com/jackc/pgx"

	"github.com/apex/log"
	"github.com/apex/log/handlers/discard"
)

var tableName = "schema_migrations"

// Log default interface discards
var Log log.Interface = &log.Logger{
	Handler: discard.Default,
	Level:   log.InfoLevel,
}

var reFile = regexp.MustCompile(`^\d\d\d_`)

type direction uint

const (
	up direction = iota
	down
)

// Up migrates the database up by n
func Up(conn *pgx.Conn, dir string, n *uint) error {
	if !exists(dir) {
		return fmt.Errorf("migrate: directory doesn't exist: %s", dir)
	}

	var i uint = math.MaxUint32
	if n != nil {
		i = *n
	}

	if err := ensureTableExists(conn); err != nil {
		return err
	}

	local, err := localVersion(dir)
	if err != nil {
		return err
	}

	remote, err := Version(conn)
	if err != nil {
		return err
	}

	files, err := migrations(dir, up)
	if err != nil {
		return err
	}

	tx, err := conn.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	remote++
	for i > 0 && remote <= local {
		name, err := at(files, remote)
		if err != nil {
			return err
		}

		migration, isset := files[name]
		if isset {
			if _, err := tx.Exec(migration); err != nil {
				return format(name, migration, err)
			}
		}

		// increment version
		if err := insertVersion(tx, remote); err != nil {
			return err
		}

		if isset {
			Log.Info(name)
		}

		// update counts
		i--
		remote++
	}

	return tx.Commit()
}

// Down migrates the database down by n
func Down(conn *pgx.Conn, dir string, n *uint) error {
	if !exists(dir) {
		return fmt.Errorf("migrate: directory doesn't exist: %s", dir)
	}

	var i uint = math.MaxUint32
	if n != nil {
		i = *n
	}

	if err := ensureTableExists(conn); err != nil {
		return err
	}

	remote, err := Version(conn)
	if err != nil {
		return err
	}

	files, err := migrations(dir, down)
	if err != nil {
		return err
	}

	tx, err := conn.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for i > 0 && remote > 0 {
		name, err := at(files, remote)
		if err != nil {
			return err
		}

		migration, isset := files[name]
		if isset {
			if _, err := tx.Exec(migration); err != nil {
				return format(name, migration, err)
			}
		}

		// delete version
		if err := deleteVersion(tx, remote); err != nil {
			return err
		}

		if isset {
			Log.Info(name)
		}

		i--
		remote--
	}

	return tx.Commit()
}

// Create a migration in dir
func Create(dir string, name string) error {
	n, err := localVersion(dir)
	if err != nil {
		return err
	}

	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	extless := path.Join(dir, pad(n+1)+"_"+name)

	// up file
	if err := ioutil.WriteFile(extless+".up.sql", []byte{}, 0644); err != nil {
		return err
	}
	Log.Infof("wrote: %s", extless+".up.sql")

	// down file
	if err := ioutil.WriteFile(extless+".down.sql", []byte{}, 0644); err != nil {
		return err
	}
	Log.Infof("wrote: %s", extless+".down.sql")

	return nil
}

func exists(dir string) bool {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		return false
	}
	return true
}

func ensureTableExists(conn *pgx.Conn) error {
	r := conn.QueryRow("SELECT count(*) FROM information_schema.tables WHERE table_name = $1 AND table_schema = (SELECT current_schema());", tableName)
	c := 0
	if err := r.Scan(&c); err != nil {
		return err
	}
	if c > 0 {
		return nil
	}
	if _, err := conn.Exec("CREATE TABLE IF NOT EXISTS " + tableName + " (version bigint not null primary key);"); err != nil {
		return err
	}
	return nil
}

// localVersion gets the latest version in the migrations dir
func localVersion(dir string) (uint, error) {
	filenames, err := readdir(dir)
	if err != nil {
		return 0, err
	} else if len(filenames) == 0 {
		return 0, nil
	}

	last := filenames[len(filenames)-1]
	return extractVersion(last)
}

// Version gets the version from postgres
func Version(conn *pgx.Conn) (version uint, err error) {
	err = conn.QueryRow("SELECT version FROM " + tableName + " ORDER BY version DESC LIMIT 1").Scan(&version)
	switch {
	case err == pgx.ErrNoRows:
		return 0, nil
	case err != nil:
		return 0, err
	default:
		return version, nil
	}
}

func insertVersion(tx *pgx.Tx, version uint) error {
	if _, err := tx.Exec("INSERT INTO "+tableName+" (version) VALUES ($1)", version); err != nil {
		return err
	}
	return nil
}

func deleteVersion(tx *pgx.Tx, version uint) error {
	if _, err := tx.Exec("DELETE FROM "+tableName+" WHERE version=$1", version); err != nil {
		return err
	}
	return nil
}

func readdir(dir string) (filenames []string, err error) {
	files, err := ioutil.ReadDir(dir)
	if err != nil && !os.IsNotExist(err) {
		return filenames, err
	}

	if len(files) == 0 {
		return filenames, nil
	}

	for _, file := range files {
		name := file.Name()
		if !reFile.MatchString(name) {
			continue
		}
		filenames = append(filenames, name)
	}
	sort.Strings(filenames)

	return filenames, nil
}

func extractVersion(filename string) (uint, error) {
	parts := strings.SplitN(filename, "_", 2)
	if len(parts) != 2 {
		return 0, fmt.Errorf("invalid migration filename: %s", filename)
	}

	n, err := strconv.Atoi(parts[0])
	if err != nil {
		return 0, err
	}

	if n < 0 {
		return 0, fmt.Errorf("migration must be higher than 0: %d", n)
	}

	return uint(n), nil
}

func migrations(dir string, d direction) (map[string]string, error) {
	filenames, err := readdir(dir)
	if err != nil {
		return nil, err
	}

	migrations := map[string]string{}
	for _, filename := range filenames {
		if d == up && !strings.Contains(filename, ".up.sql") {
			continue
		}
		if d == down && !strings.Contains(filename, ".down.sql") {
			continue
		}

		file, err := ioutil.ReadFile(path.Join(dir, filename))
		if err != nil {
			return nil, err
		}

		migrations[filename] = string(file)
	}

	return migrations, nil
}

func at(migrations map[string]string, n uint) (string, error) {
	for name := range migrations {
		v, err := extractVersion(name)
		if err != nil {
			return "", err
		}
		if n == v {
			return name, nil
		}
	}
	return "", nil
}

func pad(n uint) string {
	switch {
	case n < 10:
		return "00" + strconv.Itoa(int(n))
	case n < 100:
		return "0" + strconv.Itoa(int(n))
	default:
		return strconv.Itoa(int(n))
	}
}

func format(name string, data string, err error) error {
	switch pqErr := err.(type) {
	case pgx.PgError:
		if pqErr.Position == 0 {
			return fmt.Errorf("%s: %s", name, pqErr.Message)
		}
		return fmt.Errorf("%s:%d %s", name, line(data, int(pqErr.Position)), pqErr.Message)
	default:
		return err
	}
}

func line(data string, pos int) (line int) {
	line = 1
	for i := 0; i < pos; i++ {
		if data[i] == '\n' {
			line++
		}
	}
	return line
}
