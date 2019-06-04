package migrate

import (
	"bytes"
	"database/sql"
	"errors"
	"io"
	"time"

	"github.com/spidernest-go/db/lib/sqlbuilder"
)

var (
	tableExists  = false
	databaseName string
)

type Migration struct {
	Applied time.Time `db:"applied"`
	Version uint8     `db:"version"`
	Name    string    `db:"migration"`
}

var (
	ErrEmptyArgument  = errors.New("an argument passed is empty")
	ErrUnevenLength   = errors.New("an argument passed does not match the length of the other arguments")
	ErrMigrationOrder = errors.New("migration order supplied is not sorted properly")
)

func Apply(version uint8, name string, r io.Reader, db sqlbuilder.Database, argv ...interface{}) error {
	if err := findtable(db); err != nil {
		return err
	}

	buf := new(bytes.Buffer)
	buf.ReadFrom(r)

	stmt, err := db.Prepare(buf.String())
	if err != nil {
		return err
	}
	_, err = stmt.Exec(argv...)
	if err != nil {
		return err
	}

	// Track this migration being applied
	err = track(version, name, nil, db)
	if err != nil {
		return err
	}

	return nil
}

// Last returns the last migration applied to the database
func Last(db sqlbuilder.Database) (*Migration, error) {
	if err := findtable(db); err != nil {
		return nil, err
	}

	stmt, err := db.Prepare(`
				SELECT *
				FROM __meta
				ORDER BY applied DESC
				LIMIT 1`) // TODO: confirm as optimized as possible with ORDER BY statement existing
	if err != nil {
		return nil, err
	}

	m := new(Migration)
	err = stmt.QueryRow().Scan(&m.Applied, &m.Version, &m.Name)
	if err == sql.ErrNoRows {
		return nil, nil
	} else if err != nil {
		return nil, err
	} else {
		return m, nil
	}
}

func UpTo(v []uint8, n []string, t []time.Time, r []io.Reader, db sqlbuilder.Database) error {
	var stmt *sql.Stmt
	var err error

	// confirm table exists
	if err := findtable(db); err != nil {
		return err
	}

	// confirm valid migrations passed
	if len(v) == 0 || len(n) == 0 || len(t) == 0 || len(r) == 0 {
		return ErrEmptyArgument
	}
	if len(v) != len(n) && len(n) != len(t) && len(t) != len(r) {
		return ErrUnevenLength
	}
	m, err := Last(db)
	if err != nil {
		return err
	}
	for i := range r {
		if i == 0 { // first migraiton should check the last migration in the database
			if m != nil && !(m.Applied.Before(t[0]) || m.Version < v[0]) {
				continue
			}
		} else {
			if !(t[i-1].Before(t[i]) && v[i-1] < v[i]) {
				return ErrMigrationOrder
			}
		}
	}

	for i := range r {

		err = checkForMigration(n[i], v[i], db)
		if err == sql.ErrNoRows { // the migration doesn't already exist so lets apply it
			// read in migration
			buf := new(bytes.Buffer)
			buf.ReadFrom(r[i])

			// apply migration
			stmt, err = db.Prepare(buf.String())
			if err != nil {
				return err
			}
			_, err = stmt.Exec()
			if err != nil {
				return err
			}

			// track migration
			err = track(v[i], n[i], t[i], db)
			if err != nil {
				return err
			}
		} else if err != nil { // it was an error with checking for the migration...
			return err
		}
	}

	return nil
}

func checkForMigration(name string, version uint8, db sqlbuilder.Database) error {
	stmt, err := db.Prepare("SELECT * FROM __meta WHERE migration=? AND version=?")
	if err != nil {
		return err
	}
	m := *new(Migration)
	return stmt.QueryRow(name, version).Scan(&m.Applied, &m.Version, &m.Name)
}

func checkForMetaTable(database string, db sqlbuilder.Database) error {
	// Check if the meta table exists
	stmt, err := db.Prepare(`
        SELECT VERSION FROM information_schema.tables
        WHERE table_schema = ?
            AND table_name = ?
        LIMIT 1;`)
	if err != nil {
		return err
	}

	// If it doesn't, create it
	r := stmt.QueryRow(database, "__meta")
	t := *new(int64)
	err = r.Scan(&t)
	if err == sql.ErrNoRows {
		stmt, err := db.Prepare(`
            CREATE TABLE __meta (
                applied DATETIME NOT NULL DEFAULT NOW(),
                version TINYINT UNSIGNED,
                migration VARCHAR(256)
                )`)
		if err != nil {
			return err
		}
		_, err = stmt.Exec()
		if err != nil {
			return err
		}
		tableExists = true
		return nil
	} else if err != nil {
		// Otherwise fail.
		return err
	} else {
		return nil
	}

}

// Track this migration being applied
func track(version uint8, name string, applied interface{}, db sqlbuilder.Database) error {
	var stmt *sql.Stmt
	var err error
	switch t := applied.(type) {
	case time.Time:
		stmt, err = db.Prepare(`
			INSERT
				INTO __meta (version, migration, applied)
				VALUES (?, ?, ?)`)
		stmt.Exec(version, name, t)
	default:
		stmt, err = db.Prepare(`
			INSERT
				INTO __meta (version, migration)
				VALUES (?, ?)`)
		stmt.Exec(version, name)
	}
	if err != nil {
		return err
	}
	return nil
}

func findtable(db sqlbuilder.Database) error {
	// QUEST: This could introduce a subtle bug where two different databases from different drivers of the same name won't trigger this when one of them may not have the meta table
	// BUG: This may not work under multithreaded conditions because of global variable usage, this can be fixed by turning them into mutexes, but that will definitely make things slower.
	if databaseName != db.Name() {
		tableExists = false
	}
	if tableExists == false {
		err := checkForMetaTable(db.Name(), db)
		if err != nil {
			return err
		}
	}

	return nil
}
