package migrate

import (
	"database/sql"
	"io"

	"github.com/spidernest-go/db/lib/sqlbuilder"
)

var (
	tableExists = false
	tableName   string
)

func Apply(version uint8, name, database string, r io.Reader, db sqlbuilder.Database, argv ...interface{}) error {
	// QUEST: This could introduce a subtle bug where two different databases from different drivers of the same name won't trigger this when one of them may not have the meta table
	// BUG: This may not work under multithreaded conditions because of global variable usage, this can be fixed by turning them into mutexes, but that will definitely make things slower.
	if tableName != db.Name() {
		tableExists = false
	}
	if tableExists == false {
		err := checkForMetaTable(database, db)
		if err != nil {
			return err
		}
	}

	stmt, err := db.Prepare(r)
	if err != nil {
		return err
	}
	_, err = stmt.Exec(argv...)
	if err != nil {
		return err
	}

	// Track this migration being applied
	// ALERT: Errors won't be allocated here simply because the migration has already been applied, so there is no point.
	stmt, _ = db.Prepare(`
		INSERT
			INTO "__meta" ("version", "migration")
			VALUES (?, ?)`)
	stmt.Exec(version, name)
	return nil
}

func checkForMetaTable(database string, db sqlbuilder.Database) error {
	// Check if the meta table exists
	stmt, err := db.Prepare(`
        SELECT * FROM information_schema.tables
        WHERE table_schema = ?
            AND table_name = "__meta"
        LIMIT 1;`)
	if err != nil {
		return err
	}

	// If it doesn't, create it
	_, err = stmt.Query(database)
	if err == sql.ErrNoRows {
		stmt, err := db.Prepare(`
            CREATE TABLE "__meta" (
                "applied" DATETIME NOT NULL DEFAULT NOW(),
                "version" TINYINT UNSIGNED,
                "migration" VARCHAR(256)
                )`)
		if err != nil {
			return err
		}
		_, err = stmt.Exec()
		if err == nil {
			tableExists = true
		}
		return err
	} else {
		// Otherwise fail.
		return err
	}

}
