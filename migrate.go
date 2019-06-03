package migrate

import (
	"bytes"
	"database/sql"
	"fmt"
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

func Apply(version uint8, name string, r io.Reader, db sqlbuilder.Database, argv ...interface{}) error {
	if err := findtable(db); err != nil {
		return err
	}
	buf := new(bytes.Buffer)
	buf.ReadFrom(r)
	stmt, err := db.Prepare(buf.String())
	if err != nil {
		return fmt.Errorf("failed preparing statement '%s': %v", buf.String(), err)
	}
	_, err = stmt.Exec(argv...)
	if err != nil {
		return fmt.Errorf("failed executing query '%s': %v", buf.String(), err)
	}

	// Track this migration being applied
	// ALERT: Errors won't be allocated here simply because the migration has already been applied, so there is no point.
	track(version, name, db)
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
		return nil, fmt.Errorf("failed preparing meta table lookup statement: %v", err)
	}
	m := new(Migration)
	err = stmt.QueryRow().Scan(&m.Applied, &m.Version, &m.Name)
	if err == sql.ErrNoRows {
		return nil, nil
	} else if err != nil {
		return nil, fmt.Errorf("failed querying last migration: %v", err)
	} else {
		return m, nil
	}
}

func UpTo(v []uint8, n []string, t []time.Time, r []io.Reader, db sqlbuilder.Database) error {
	// confirm table exists
	if err := findtable(db); err != nil {
		return err
	}

	// confirm valid migrations passed
	if len(v) == 0 || len(n) == 0 || len(t) == 0 || len(r) == 0 {
		return fmt.Errorf("an argument array passed into function")
	}
	if len(v) != len(n) && len(n) != len(t) && len(t) != len(r) {
		return fmt.Errorf("argument array lengths are uneven")
	}
	m, err := Last(db)
	if err != nil {
		return err
	}
	for i := range r {
		if i == 0 { // first migraiton should check the last migration in the database
			if m != nil && !(m.Applied.Before(t[0]) || m.Version < v[0]) {
				return fmt.Errorf("migration 0 does not occur after the last migration in the database")
			}
		} else {
			if !(t[i-1].Before(t[i]) || v[i-1] < v[i]) {
				return fmt.Errorf("migration %d does not occur after migration %d", i, i-1)
			}
		}
	}

	for i := range r {
		// read in migration
		buf := new(bytes.Buffer)
		buf.ReadFrom(r[i])

		// apply migration
		stmt, err := db.Prepare(buf.String())
		if err != nil {
			return fmt.Errorf("migration %d failed preparing statement %s: %v", i, buf.String(), err)
		}
		_, err = stmt.Exec()
		if err != nil {
			return fmt.Errorf("migration %d failed executing statement %s: %v", i, buf.String(), err)
		}

		// track migration
		track(v[i], n[i], db)
	}

	return nil
}

func checkForMetaTable(database string, db sqlbuilder.Database) error {
	// Check if the meta table exists
	stmt, err := db.Prepare(`
        SELECT VERSION FROM information_schema.tables
        WHERE table_schema = ?
            AND table_name = ?
        LIMIT 1;`)
	if err != nil {
		return fmt.Errorf("error preparing information_schema table query: %v", err)
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
			return fmt.Errorf("error preparing meta table prepare statement: %v", err)
		}
		_, err = stmt.Exec()
		if err != nil {
			return fmt.Errorf("error in executing meta table creation statement: %v", err)
		}
		tableExists = true
		return nil
	} else if err != nil {
		// Otherwise fail.
		return fmt.Errorf("error scanning meta table: %v", err)
	} else {
		return nil
	}

}

// Track this migration being applied
func track(version uint8, name string, db sqlbuilder.Database) {
	// ALERT: Errors won't be allocated here simply because the migration has already been applied, so there is no point.
	stmt, _ := db.Prepare(`
		INSERT
			INTO __meta (version, migration)
			VALUES (?, ?)`)
	stmt.Exec(version, name)
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
