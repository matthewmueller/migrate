package migrate

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"

	// postgres
	_ "github.com/lib/pq"

	"github.com/apex/log"
	"github.com/apex/log/handlers/discard"
)

var tableName = "schema_migrations"

// Log default interface discards
var Log log.Interface = &log.Logger{
	Handler: discard.Default,
	Level:   log.InfoLevel,
}

type direction uint

const (
	up direction = iota
	down
)

// Up migrates the database up by n
func Up(db *sql.DB, dir string, n *uint) error {
	if !exists(dir) {
		return fmt.Errorf("migrate: directory doesn't exist: %s", dir)
	}

	var i uint = math.MaxUint32
	if n != nil {
		i = *n
	}

	if err := ensureTableExists(db); err != nil {
		return err
	}

	local, err := localVersion(dir)
	if err != nil {
		return err
	}

	remote, err := Version(db)
	if err != nil {
		return err
	}

	files, err := migrations(dir, up)
	if err != nil {
		return err
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	remote++
	for i > 0 && remote <= local {
		migration, isset := files[remote]
		if isset {
			if _, err := tx.Exec(migration); err != nil {
				return err
			}
		}

		// increment version
		if err := insertVersion(tx, remote); err != nil {
			return err
		}

		// update counts
		i--
		remote++
	}

	return tx.Commit()
}

// Down migrates the database down by n
func Down(db *sql.DB, dir string, n *uint) error {
	if !exists(dir) {
		return fmt.Errorf("migrate: directory doesn't exist: %s", dir)
	}

	var i uint = math.MaxUint32
	if n != nil {
		i = *n
	}

	if err := ensureTableExists(db); err != nil {
		return err
	}

	remote, err := Version(db)
	if err != nil {
		return err
	}

	files, err := migrations(dir, down)
	if err != nil {
		return err
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for i > 0 && remote > 0 {
		migration, isset := files[remote]
		if isset {
			if _, err := tx.Exec(migration); err != nil {
				return err
			}
		}

		// delete version
		if err := deleteVersion(tx, remote); err != nil {
			return err
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

func ensureTableExists(db *sql.DB) error {
	r := db.QueryRow("SELECT count(*) FROM information_schema.tables WHERE table_name = $1 AND table_schema = (SELECT current_schema());", tableName)
	c := 0
	if err := r.Scan(&c); err != nil {
		return err
	}
	if c > 0 {
		return nil
	}
	if _, err := db.Exec("CREATE TABLE IF NOT EXISTS " + tableName + " (version bigint not null primary key);"); err != nil {
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
func Version(db *sql.DB) (version uint, err error) {
	err = db.QueryRow("SELECT version FROM " + tableName + " ORDER BY version DESC LIMIT 1").Scan(&version)
	switch {
	case err == sql.ErrNoRows:
		return 0, nil
	case err != nil:
		return 0, err
	default:
		return version, nil
	}
}

func insertVersion(tx *sql.Tx, version uint) error {
	if _, err := tx.Exec("INSERT INTO "+tableName+" (version) VALUES ($1)", version); err != nil {
		return err
	}
	return nil
}

func deleteVersion(tx *sql.Tx, version uint) error {
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
		filenames = append(filenames, file.Name())
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

func migrations(dir string, d direction) (map[uint]string, error) {
	filenames, err := readdir(dir)
	if err != nil {
		return nil, err
	}

	migrations := map[uint]string{}
	for _, filename := range filenames {
		if d == up && !strings.Contains(filename, ".up.sql") {
			continue
		}
		if d == down && !strings.Contains(filename, ".down.sql") {
			continue
		}

		n, err := extractVersion(filename)
		if err != nil {
			return nil, err
		}

		file, err := ioutil.ReadFile(path.Join(dir, filename))
		if err != nil {
			return nil, err
		}

		migrations[n] = string(file)
	}

	return migrations, nil
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
