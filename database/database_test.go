package database

// @Title        database_test.go
// @Description
// @Create       david 2025-01-09 14:17
// @Update       david 2025-01-09 14:17
import (
	"godb/logger"
	"strings"
	"testing"
)

func TestDatabase(t *testing.T) {
	// 设置日志级别
	logger.SetLevel(logger.INFO)
	dir := "data"
	base := NewDataBase(dir)

	logger.Info(":::start to test database......")
	// 创建表
	_, err := base.Execute("CREATE TABLE users (id INT PRIMARY KEY, name CHAR, age INT INDEX, email CHAR, score INT INDEX, status CHAR)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// 插入数据
	inserts := []string{
		"INSERT INTO users VALUES (1, 'Alice', 25, 'alice@email.com', 95, 'active')",
		"INSERT INTO users VALUES (2, 'Bob', 30, 'bob@email.com', 88, 'active')",
		"INSERT INTO users VALUES (3, 'Charlie', 25, 'charlie@email.com', 92, 'inactive')",
		"INSERT INTO users VALUES (4, 'David', 35, 'david@email.com', 78, 'active')",
		"INSERT INTO users VALUES (5, 'Eve', 28, 'eve@email.com', 90, 'active')",
	}

	for _, insert := range inserts {
		base.Execute(insert)
	}

	// 主键查询测试
	logger.Info(strings.Repeat("-", 80))
	logger.Info(":::start to QUERY main keys......")
	result, err := base.Execute("SELECT id, name, age FROM users WHERE id = 1")
	if err != nil {
		t.Fatalf("Failed to query by primary key: %v", err)
	}
	logger.Info("Primary key query result: %v", result)

	// 二级索引查询测试 - Age
	logger.Info(strings.Repeat("-", 80))
	logger.Info(":::start to QUERY secondary keys......")
	result, err = base.Execute("SELECT id, name, email FROM users WHERE age = 25")
	if err != nil {
		t.Fatalf("Failed to query by age index: %v", err)
	}
	logger.Info("Age index query result:\n%v", result)

	// 二级索引查询测试 - Socre
	result, err = base.Execute("SELECT id, name, email FROM users WHERE score = 90")
	if err != nil {
		t.Fatalf("Failed to query by email index: %v", err)
	}
	logger.Info("Email index query result:\n%v", result)

	// 复合条件查询测试
	logger.Info(strings.Repeat("-", 80))
	logger.Info(":::start to complex QUERY keys......")
	result, err = base.Execute("SELECT id, name, age, status FROM users WHERE age = 25 AND status = 'active'")
	if err != nil {
		t.Fatalf("Failed to query with multiple conditions: %v", err)
	}
	logger.Info("Multiple conditions query result:\n%v", result)

	// 范围查询测试
	logger.Info(strings.Repeat("-", 80))
	logger.Info(":::start to range QUERY keys......")
	result, err = base.Execute("SELECT id, name, age, score FROM users WHERE age >= 25 AND age <= 30")
	if err != nil {
		t.Fatalf("Failed to perform range query: %v", err)
	}
	logger.Info("Range query result:\n%v", result)

	// 更新测试
	logger.Info(strings.Repeat("-", 80))
	logger.Info(":::start to test UPDATE......")
	_, err = base.Execute("UPDATE users SET status = 'inactive' WHERE id = 2")
	if err != nil {
		t.Fatalf("Failed to update record: %v", err)
	}

	// 验证更新结果
	result, err = base.Execute("SELECT id, name, status FROM users WHERE id = 2")
	if err != nil {
		t.Fatalf("Failed to verify update: %v", err)
	}
	logger.Info("Update verification result: %v", result)

	//// 删除测试
	//_, err = base.Execute("DELETE FROM users WHERE id = 5")
	//if err != nil {
	//	t.Fatalf("Failed to delete record: %v", err)
	//}
	//
	//// 验证删除结果
	//result, err = base.Execute("SELECT id, name FROM users WHERE id = 5")
	//if err != nil {
	//	t.Fatalf("Failed to verify deletion: %v", err)
	//}
	//logger.Info("Delete verification result: %v", result)
	//
	//// 聚合查询测试
	//queries := []string{
	//	"SELECT COUNT(*) FROM users",
	//	"SELECT MAX(age) FROM users",
	//	"SELECT MIN(score) FROM users",
	//	"SELECT AVG(age) FROM users",
	//}
	//
	//for _, query := range queries {
	//	result, err := base.Execute(query)
	//	if err != nil {
	//		logger.Info("Aggregate query not supported: %v", err)
	//		continue
	//	}
	//	logger.Info("Aggregate query result (%s): %v", query, result)
	//}
}
