package database

// @Title        database_test.go
// @Description
// @Create       david 2025-01-09 14:17
// @Update       david 2025-01-09 14:17
import (
	"testing"
)

func TestDatabase(t *testing.T) {
	base := NewWebpDataBase("test_db.db")

	base.Execute("select * from testTable")
}
