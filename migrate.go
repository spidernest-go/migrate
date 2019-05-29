package migrate

import (
	"database/sql"

	"github.com/spidernest-go/db"
	"github.com/spidernest-go/db/lib/sqlbuilder"
)

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
		return err
	} else {
		// Otherwise fail.
		return err
	}

}
