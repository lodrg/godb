package database

// @Title        database_test.go
// @Description
// @Create       david 2025-01-09 14:17
// @Update       david 2025-01-09 14:17
import (
	"testing"
)

func TestDatabase(t *testing.T) {
	dir := "data"
	//resetDataDirectory(dir)
	base := NewWebpDataBase(dir)
	base.Execute("select * from testTable")
}
