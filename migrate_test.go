package migrate

import (
	"bytes"
	"os"
	"testing"

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

var TestEntry = bytes.NewBufferString("UPDATE * FROM users WHERE id = 1;")

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
}

func TestApply(t *testing.T) {
	clear()
	assert.NoError(t, Apply(0, "some name", TestEntry, Builder))
}

