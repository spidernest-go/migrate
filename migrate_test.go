package migrate

import (
	"bytes"
	"io"
	"os"
	"testing"
	"time"

	"github.com/spidernest-go/db/lib/sqlbuilder"
	"github.com/spidernest-go/db/mysql"
	"github.com/stretchr/testify/assert"
)

var Builder sqlbuilder.Database
var Settings = mysql.ConnectionURL{
	Database: "migrate_test",
	Host:     os.Getenv("MYSQL_HOST"),
	User:     os.Getenv("MYSQL_USER"),
	Password: os.Getenv("MYSQL_PASS"),
}
var MigrationName []string
var GoodEntry []*bytes.Buffer
var BadEntry []*bytes.Buffer

func TestMain(m *testing.M) {
	var err error
	Builder, err = mysql.Open(Settings)
	if err != nil {
		panic(err)
	}
	defer Builder.Close()
	os.Exit(m.Run())
}

func clear() {
	Builder.Exec("DROP TABLE __meta")
	Builder.Exec("DROP TABLE users")
	MigrationName = []string{
		"Creating the users table.",
		"Root admin created.",
		"Add column 'admin' which determines administrative status.",
		"Give 'Root' admin status.",
	}
	GoodEntry = []*bytes.Buffer{
		bytes.NewBufferString("CREATE TABLE users(username TEXT NOT NULL)"),
		bytes.NewBufferString("INSERT INTO users(username) VALUES (\"root\")"),
		bytes.NewBufferString("ALTER TABLE users ADD admin BOOL NOT NULL DEFAULT 0"),
		bytes.NewBufferString("UPDATE users SET admin=1 WHERE username=\"root\""),
	}
	BadEntry = []*bytes.Buffer{
		bytes.NewBufferString("just a bad sql statement :)"),
	}
}

func TestApply(t *testing.T) {
	clear()
	assert.Error(t, Apply(0, MigrationName[0], BadEntry[0], Builder), "We expect an error here because a bad sql statement is given.")
	assert.NoError(t, Apply(0, MigrationName[0], GoodEntry[0], Builder), "We expect no error.")
	assert.Error(t, Apply(0, MigrationName[0], GoodEntry[0], Builder), "We expect an error here because a duplicate version number is given.")
	assert.Error(t, Apply(1, MigrationName[0], GoodEntry[0], Builder), "We expect an error here because a duplicate migration name is given.")
	assert.NoError(t, Apply(1, MigrationName[1], GoodEntry[1], Builder), "We expect no error.")
	assert.NoError(t, Apply(1, MigrationName[2], GoodEntry[2], Builder), "We expect no error.")
	assert.NoError(t, Apply(1, MigrationName[3], GoodEntry[3], Builder), "We expect no error.")
}

func TestLast(t *testing.T) {
	clear()
	migrationName := MigrationName[0]
	migrationVer := uint8(0)

	lastMigration, err := Last(Builder)
	assert.Nil(t, lastMigration, "If there are no migrations the result should be nil for the returned migration.")
	assert.NoError(t, err, "No error should occur when the table is queryed for a migration if none exist.")

	// Put something in the __meta database to play with
	assert.NoError(t, Apply(0, migrationName, GoodEntry[0], Builder), "We expected apply to work but it did not.")

	lastMigration, err = Last(Builder)
	if assert.NotNil(t, lastMigration, "We expected that a migration would be return and it did not get returned.") {
		assert.NotNil(t, lastMigration.Applied, "We expected the migration application time in the meta table to be something.")
		assert.Equal(t, lastMigration.Name, migrationName[0], "We expected the migration name in the meta table to be the same as the one we just applied.")
		assert.Equal(t, lastMigration.Version, migrationVer, "We expected the migration version in the meta table to be the same as the one we just applied.")
	}
	assert.NoError(t, err, "We expected no error retrieving the last migration and we expected one to exist since we just added one.")
}

func TestUpTo(t *testing.T) {
	clear()
	basetime := time.Now()

	// Out of order versions
	versions := []uint8{7, 1, 2, 3}
	times := []time.Time{basetime.AddDate(0, 0, 0), basetime.AddDate(1, 0, 0), basetime.AddDate(2, 0, 0), basetime.AddDate(3, 0, 0)}
	readers := []io.Reader{GoodEntry[0], GoodEntry[1], GoodEntry[2], GoodEntry[3]}
	assert.Error(t, UpTo(versions, MigrationName, times, readers, Builder), "We expect this to work with no error.")

	// Out of order dates
	versions = []uint8{0, 1, 2, 3}
	times = []time.Time{basetime.AddDate(0, 0, 0), basetime.AddDate(5, 0, 0), basetime.AddDate(2, 0, 0), basetime.AddDate(3, 0, 0)}
	readers = []io.Reader{GoodEntry[0], GoodEntry[1], GoodEntry[2], GoodEntry[3]}
	assert.Error(t, UpTo(versions, MigrationName, times, readers, Builder), "We expect this to work with no error.")

	// Out of order versions and dates
	versions = []uint8{7, 1, 2, 3}
	times = []time.Time{basetime.AddDate(0, 0, 0), basetime.AddDate(5, 0, 0), basetime.AddDate(2, 0, 0), basetime.AddDate(3, 0, 0)}
	readers = []io.Reader{GoodEntry[0], GoodEntry[1], GoodEntry[2], GoodEntry[3]}
	assert.Error(t, UpTo(versions, MigrationName, times, readers, Builder), "We expect this to work with no error.")

	// Good request
	versions = []uint8{0, 1, 2, 3}
	times = []time.Time{basetime.AddDate(0, 0, 0), basetime.AddDate(1, 0, 0), basetime.AddDate(2, 0, 0), basetime.AddDate(3, 0, 0)}
	readers = []io.Reader{GoodEntry[0], GoodEntry[1], GoodEntry[2], GoodEntry[3]}
	assert.NoError(t, UpTo(versions, MigrationName, times, readers, Builder), "We expect this to work with no error.")
}
