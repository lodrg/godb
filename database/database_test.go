package database

// @Title        database_test.go
// @Description
// @Create       david 2025-01-09 14:17
// @Update       david 2025-01-09 14:17
import (
	"fmt"
	"testing"
)

func TestDatabase(t *testing.T) {
	dir := "data"
	//resetDataDirectory(dir)
	base := NewWebpDataBase(dir)
	base.Execute("insert into testTable values (1,'12')")

	result, err := base.Execute("select id,name from testTable where id = 1")

	if err != nil {
		t.Error(err)
	}
	fmt.Printf("result: %v\n", result)
	for s, i := range result.rows {
		fmt.Printf("row[%v]=%v\n", s, i)
	}
}
