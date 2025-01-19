package migrate

import (
	"database/sql"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"math"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgconn"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/matthewmueller/logs"
	"github.com/matthewmueller/migrate/internal/dedent"
	"github.com/matthewmueller/text"
	"github.com/xo/dburl"

	// sqlite db
	_ "github.com/mattn/go-sqlite3"
)

var reFile = regexp.MustCompile(`^\d\d\d_`)
var sep = string(os.PathSeparator)

// var tableName = "migrate"

// ErrZerothMigration occurs when the migrations start at 000
var ErrZerothMigration = errors.New("migrations should start at 001 not 000")

// ErrNoMigrations happens when there are no migrations
var ErrNoMigrations = errors.New("no migrations")

// ErrNotEnoughMigrations happens when your migrations folder has less migrations than remote's version
var ErrNotEnoughMigrations = errors.New("remote migration version greater than the number of migrations you have")

// File is a writable file
type File interface {
	fs.FS
	io.Writer
}

// Connect to a database depending on the URL schema
func Connect(conn string) (db *sql.DB, err error) {
	url, err := dburl.Parse(conn)
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

// New creates a new migrations in dir
// TODO: figure out a writable virtual file-system for this
func New(log *slog.Logger, dir string, name string) error {
	log = logger(log)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	files, err := getFilesFromOS(dir)
	if err != nil {
		return err
	}
	migrations, err := upMigrations(files)
	if err != nil {
		return err
	}
	var latest uint
	if len(migrations) > 0 {
		latest = migrations[len(migrations)-1].Version
	}
	base := path.Join(dir, pad(latest+1)+"_"+text.Snake(name))

	// up file
	if err := os.WriteFile(base+".up.sql", []byte{}, 0644); err != nil {
		return err
	}
	log.Info("wrote: " + base + ".up.sql")

	// down file
	if err := os.WriteFile(base+".down.sql", []byte{}, 0644); err != nil {
		return err
	}
	log.Info("wrote: " + base + ".down.sql")

	return nil
}

// Migration struct
type Migration struct {
	Name    string
	Code    string
	Dir     Direction
	Version uint
}

// LocalVersion fetches the latest local version
func LocalVersion(fs fs.FS) (name string, err error) {
	files, err := getFiles(fs)
	if err != nil {
		return name, err
	}
	migrations, err := upMigrations(files)
	if err != nil {
		return name, err
	} else if len(migrations) == 0 {
		return name, ErrNoMigrations
	}
	migration := migrations[len(migrations)-1]
	return migration.Name, nil
}

// RemoteVersion fetches the latest local version
func RemoteVersion(db *sql.DB, fs fs.FS, tableName string) (name string, err error) {
	if err := ensureTableExists(db, tableName); err != nil {
		return name, err
	}
	remote, err := getRemoteVersion(db, tableName)
	if err != nil {
		return name, err
	} else if remote == 0 {
		return name, ErrNoMigrations
	}
	files, err := getFiles(fs)
	if err != nil {
		return name, err
	}
	migrations, err := upMigrations(files)
	if err != nil {
		return name, err
	} else if len(migrations) == 0 {
		return name, ErrNoMigrations
	}
	migration := migrations[remote-1]
	return migration.Name, nil
}

// Up migrates the database up to the latest migration
func Up(log *slog.Logger, db *sql.DB, fs fs.FS, tableName string) error {
	log = logger(log)
	return UpBy(log, db, fs, tableName, math.MaxInt32)
}

// UpBy migrations the database up by i
func UpBy(log *slog.Logger, db *sql.DB, fs fs.FS, tableName string, i int) error {
	log = logger(log)
	files, err := getFiles(fs)
	if err != nil {
		return err
	}
	if len(files) == 0 {
		return ErrNoMigrations
	}
	migrations, err := upMigrations(files)
	if err != nil {
		return err
	}
	if len(migrations) == 0 {
		return ErrNoMigrations
	}
	if err := ensureTableExists(db, tableName); err != nil {
		return err
	}
	remote, err := getRemoteVersion(db, tableName)
	if err != nil {
		return err
	}
	local := migrations[len(migrations)-1].Version
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// next remote
	remote++
	for i > 0 && remote <= local {
		migration := migrations[remote-1]

		// execute the migration code
		if _, err := tx.Exec(migration.Code); err != nil {
			return format(migration, err)
		}

		// increment version
		if err := insertVersion(tx, tableName, remote); err != nil {
			return err
		}

		// log the next migration
		log.Info(migration.Name)

		// update counts
		i--
		remote++
	}

	return tx.Commit()
}

// Down migrates the database down to 0
func Down(log *slog.Logger, db *sql.DB, fs fs.FS, tableName string) error {
	log = logger(log)
	return DownBy(log, db, fs, tableName, math.MaxInt32)
}

// DownBy migrations the database down by i
func DownBy(log *slog.Logger, db *sql.DB, fs fs.FS, tableName string, i int) error {
	log = logger(log)
	files, err := getFiles(fs)
	if err != nil {
		return err
	}
	if len(files) == 0 {
		return ErrNoMigrations
	}
	migrations, err := downMigrations(files)
	if err != nil {
		return err
	}
	if len(migrations) == 0 {
		return ErrNoMigrations
	}
	if err := ensureTableExists(db, tableName); err != nil {
		return err
	}
	// get the current remote
	remote, err := getRemoteVersion(db, tableName)
	if err != nil {
		return err
	}
	// get the earliest local
	local := migrations[0].Version

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// next remote
	for i > 0 && remote >= local {
		if len(migrations) < int(remote) {
			return ErrNotEnoughMigrations
		}
		migration := migrations[remote-1]

		// execute the migration code
		if _, err := tx.Exec(migration.Code); err != nil {
			return format(migration, err)
		}

		// increment version
		if err := deleteVersion(tx, tableName, remote); err != nil {
			return err
		}

		// log the next migration
		log.Info(migration.Name)

		// update counts
		i--
		remote--
	}

	return tx.Commit()
}

func exists(fs fs.FS, path string) error {
	if _, err := fs.Open(path); os.IsNotExist(err) {
		return ErrNoMigrations
	} else if err != nil {
		return err
	}
	return nil
}

func getFiles(fsys fs.FS) (files map[string]string, err error) {
	if err := exists(fsys, "."); err != nil {
		return files, err
	}
	files = make(map[string]string)
	walk := func(path string, info fs.DirEntry, err error) error {
		if err != nil {
			return err
		} else if info.IsDir() {
			return nil
		}
		file, err := fsys.Open(path)
		if err != nil {
			return err
		}
		buf, err := io.ReadAll(file)
		if err != nil {
			return err
		}
		normpath := strings.Trim(path, sep)
		files[normpath] = strings.TrimSpace(dedent.String(string(buf)))
		return err
	}
	if err := fs.WalkDir(fsys, ".", walk); err != nil {
		return files, err
	}
	return files, nil
}

func getFilesFromOS(dir string) (files map[string]string, err error) {
	files = make(map[string]string)
	walk := func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		} else if info.IsDir() {
			return nil
		}
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		buf, err := io.ReadAll(file)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		normpath := strings.Trim(rel, sep)
		files[normpath] = strings.TrimSpace(dedent.String(string(buf)))
		return err
	}
	if err := filepath.Walk(dir, walk); err != nil {
		return files, err
	}
	return files, nil
}

// get the version
func getVersion(filename string) (uint, error) {
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

// get the direction
func getDirection(filename string) (Direction, error) {
	if strings.Contains(filename, "."+string(up)+".") {
		return up, nil
	}
	if strings.Contains(filename, "."+string(down)+".") {
		return down, nil
	}
	return "", errors.New("filepath must specify the direction up or down (e.g. 000_setup.up.sql)")
}

// ensure the table exists
func ensureTableExists(db *sql.DB, tableName string) error {
	if _, err := db.Exec("CREATE TABLE IF NOT EXISTS " + tableName + " (version bigint not null primary key);"); err != nil {
		return err
	}
	return nil
}

// Version gets the version from postgres
func getRemoteVersion(db *sql.DB, tableName string) (version uint, err error) {
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

// insert a new version into the table
func insertVersion(tx *sql.Tx, tableName string, version uint) error {
	if _, err := tx.Exec("INSERT INTO "+tableName+" (version) VALUES ($1)", version); err != nil {
		return err
	}
	return nil
}

// delete a version from the table
func deleteVersion(tx *sql.Tx, tableName string, version uint) error {
	if _, err := tx.Exec("DELETE FROM "+tableName+" WHERE version=$1", version); err != nil {
		return err
	}
	return nil
}

// pad a migration
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

// format a migrations error message
func format(migration *Migration, err error) error {
	name := migration.Name
	code := migration.Code
	switch pgErr := err.(type) {
	case *pgconn.PgError:
		var line uint
		var col uint
		var lineColOK bool
		line, col, lineColOK = computeLineFromPos(code, int(pgErr.Position))

		fmt.Println("line", line, "col", col, "lineColOK", lineColOK, "pgErr.Position", pgErr.Error())
		// if pgErr.Position != "" {
		// 	if pos, err := strconv.ParseUint(pgErr.Position, 10, 64); err == nil {
		// 	}
		// }
		message := fmt.Sprintf("%s failed. %s", name, pgErr.Message)
		if lineColOK {
			message = fmt.Sprintf("%s on column %d", message, col)
		}
		if pgErr.Detail != "" {
			message = fmt.Sprintf("%s, %s", message, pgErr.Detail)
		}
		return Error{OrigErr: err, Err: message, Line: line}
	default:
		return Error{OrigErr: err, Err: "migration failed", Query: []byte(code)}
	}
}

func computeLineFromPos(s string, pos int) (line uint, col uint, ok bool) {
	// replace crlf with lf
	s = strings.Replace(s, "\r\n", "\n", -1)
	// pg docs: pos uses index 1 for the first character, and positions are measured in characters not bytes
	runes := []rune(s)
	if pos > len(runes) {
		return 0, 0, false
	}
	sel := runes[:pos]
	line = uint(runesCount(sel, newLine) + 1)
	col = uint(pos - 1 - runesLastIndex(sel, newLine))
	return line, col, true
}

const newLine = '\n'

func runesCount(input []rune, target rune) int {
	var count int
	for _, r := range input {
		if r == target {
			count++
		}
	}
	return count
}

func runesLastIndex(input []rune, target rune) int {
	for i := len(input) - 1; i >= 0; i-- {
		if input[i] == target {
			return i
		}
	}
	return -1
}

// Error should be used for errors involving queries ran against the database
type Error struct {
	// Optional: the line number
	Line uint

	// Query is a query excerpt
	Query []byte

	// Err is a useful/helping error message for humans
	Err string

	// OrigErr is the underlying error
	OrigErr error
}

func (e Error) Error() string {
	if len(e.Err) == 0 {
		return fmt.Sprintf("%v in line %v: %s", e.OrigErr, e.Line, e.Query)
	}
	return fmt.Sprintf("%v in line %v: %s (details: %v)", e.Err, e.Line, e.Query, e.OrigErr)
}

// Direction string
type Direction string

// Directions
const (
	up   Direction = "up"
	down Direction = "down"
)

func upMigrations(files map[string]string) (migs []*Migration, err error) {
	return toMigrations(files, up)
}

func downMigrations(files map[string]string) (migs []*Migration, err error) {
	return toMigrations(files, down)
}

// migrations takes a file map and turns it into a sorted list of migrations
func toMigrations(files map[string]string, d Direction) (migs []*Migration, err error) {
	for path, code := range files {
		if !reFile.MatchString(path) {
			continue
		}
		n, err := getVersion(path)
		if err != nil {
			return nil, err
		}
		if n == 0 {
			return nil, ErrZerothMigration
		}
		dir, err := getDirection(path)
		if err != nil {
			return nil, err
		}
		if dir != d {
			continue
		}
		migs = append(migs, &Migration{
			Name:    path,
			Dir:     dir,
			Code:    code,
			Version: n,
		})
	}
	sort.Slice(migs, func(i, j int) bool {
		return migs[i].Version < migs[j].Version
	})
	return migs, nil
}

func logger(l *slog.Logger) *slog.Logger {
	if l == nil {
		return logs.Discard()
	}
	return l
}
