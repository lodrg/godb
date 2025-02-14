package disktree

import (
	f "godb/file"
	"godb/logger"
	"testing"
)

func TestTree(t *testing.T) {
	logger.SetLevel(logger.INFO)
	dbfileName := "test_disk.db"
	diskPager, err := f.NewDiskPager(dbfileName, 80, 80)

	redolog, err := NewRedoLog("test.log")
	if err != nil {
		t.Fatal(err)
	}

	if err != nil {
		t.Fatalf("Failed to create disk pager: %v", err)
	}
	// 创建一个4阶B+树
	tree := NewBPTree(4, 20, diskPager, redolog)

	// 测试插入多条数据
	t.Run("Insert Multiple Records", func(t *testing.T) {
		inserts := []struct {
			key   uint32
			value string
		}{
			{1, "alice@test.com"},
			{2, "bob@test.com"},
			{3, "charlie@test.com"},
			{4, "david@test.com"},
			{5, "eve@test.com"},
		}

		for _, insert := range inserts {
			tree.Insert(insert.key, []byte(insert.value))
		}
	})

	// 测试主键查询
	t.Run("Query by Primary Key", func(t *testing.T) {
		value, found := tree.Search(1)
		if !found {
			t.Fatalf("Failed to query by primary key")
		}
		logger.Info("Primary key query result: %s", string(value.([]byte)))
	})

	// 测试二级索引查询（相同值的查询）
	t.Run("Query by Secondary Index", func(t *testing.T) {
		// 先插入一些相同值的记录
		tree.Insert(6, []byte("active"))
		tree.Insert(7, []byte("active"))
		tree.Insert(8, []byte("inactive"))

		result, found := tree.SearchAll(6)
		if !found {
			t.Fatalf("Failed to query by secondary index")
		}
		logger.Info("Secondary index query result: %v", result)
	})

	// 测试更新操作
	t.Run("Update Records", func(t *testing.T) {
		tree.Insert(2, []byte("updated@test.com"))

		// 验证更新结果
		result, found := tree.Search(2)
		if !found {
			t.Fatalf("Failed to verify update")
		}
		logger.Info("Update verification result: %s", string(result.([]byte)))
	})

	// 测试大量数据插入
	t.Run("Insert Large Dataset", func(t *testing.T) {
		for i := uint32(10); i < 20; i++ {
			tree.Insert(i, []byte("user@test.com"))
		}
		tree.Print()
	})

	// 测试范围查询（如果支持的话）
	t.Run("Range Query", func(t *testing.T) {
		// 注意：这里需要实现范围查询功能
		// result := tree.RangeSearch(1, 5)
		// logger.Info("Range query result: %v", result)
	})

	// 打印最终的树结构
	tree.Print()

}
