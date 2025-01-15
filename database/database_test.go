package database

// @Title        database_test.go
// @Description
// @Create       david 2025-01-09 14:17
// @Update       david 2025-01-09 14:17
import (
	"godb/logger"
	"testing"
)

func TestDatabase(t *testing.T) {
	// 设置日志级别
	logger.SetLevel(logger.INFO)

	dir := "data"

	//resetDataDirectory(dir)
	base := NewWebpDataBase(dir)

	base.Execute("create table testTable2 (id INT PRIMARY KEY, name CHAR, age INT)")

	base.Execute("insert into testTable2 values (1,'这里',22)")

	result, _ := base.Execute("select id,name from testTable2 where id = 1")

	logger.Info("result: %v\n", result)

}
