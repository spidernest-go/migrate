package migrate

import (
	"os"
	"testing"

	"github.com/spidernest-go/db/lib/sqlbuilder"
	"github.com/spidernest-go/db/mysql"
)

var Builder sqlbuilder.Database
var Settings = mysql.ConnectionURL{
	Database: "migrate_test",
	Host:     os.Getenv("MYSQL_HOST"),
	User:     os.Getenv("MYSQL_USER"),
	Password: os.Getenv("MYSQL_PASS"),
}

func TestMain(m *testing.M) {
	var err error
	Builder, err = mysql.Open(Settings)
	if err != nil {
		panic(err)
	}
	os.Exit(m.Run())
}
