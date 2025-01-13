package database

// @Title        database_test.go
// @Description
// @Create       david 2025-01-09 14:17
// @Update       david 2025-01-09 14:17
import (
	"fmt"
	"godb/logger"
	"testing"
)

func TestDatabase(t *testing.T) {
	// 设置日志级别
	logger.SetLevel(logger.DEBUG)

	dir := "data"
	//resetDataDirectory(dir)
	base := NewWebpDataBase(dir)
	base.Execute("insert into testTable values (1,'12')")
	//base.Execute("insert into testTable values (2,'22')")

	result, err := base.Execute("select id,name from testTable where id = 1")

	if err != nil {
		t.Error(err)
	}
	fmt.Printf("result: %v\n", result)
	for s, i := range result.rows {
		fmt.Printf("row[%v]=%v\n", s, i)
	}

	result2, err := base.Execute("select id,name from testTable where id = 1")

	if err != nil {
		t.Error(err)
	}

	for s, i := range result2.rows {
		fmt.Printf("row[%v]=%v\n", s, i)
	}
}
