package file

import (
	"bytes"
	"testing"
)

func TestDiskPager(t *testing.T) {
	// 初始化参数
	filename := "test.db"
	pageSize := 4096
	cacheSize := 10

	// 创建DiskPager实例
	pager, err := NewDiskPager(filename, pageSize, cacheSize)
	if err != nil {
		t.Fatalf("Failed to create DiskPager: %v", err)
	}
	defer pager.Close()

	// 测试分配新页面
	t.Run("Allocate New Page", func(t *testing.T) {
		pageNum, err := pager.AllocateNewPage()
		if err != nil {
			t.Errorf("Failed to allocate new page: %v", err)
		}
		if pageNum != 0 {
			t.Errorf("Expected first page number to be 0, got %d", pageNum)
		}
	})

	// 测试写入页面
	t.Run("Write Page", func(t *testing.T) {
		testData := make([]byte, pageSize)
		for i := 0; i < pageSize; i++ {
			testData[i] = byte(i % 256)
		}

		err := pager.WritePage(0, testData)
		if err != nil {
			t.Errorf("Failed to write page: %v", err)
		}
	})

	// 测试读取页面
	t.Run("Read Page", func(t *testing.T) {
		data, err := pager.ReadPage(0)
		if err != nil {
			t.Errorf("Failed to read page: %v", err)
		}

		expectedData := make([]byte, pageSize)
		for i := 0; i < pageSize; i++ {
			expectedData[i] = byte(i % 256)
		}

		if !bytes.Equal(data, expectedData) {
			t.Error("Read data doesn't match written data")
		}
	})

	// 测试多页面操作
	t.Run("Multiple Pages", func(t *testing.T) {
		// 分配第二个页面
		pageNum2, err := pager.AllocateNewPage()
		if err != nil {
			t.Errorf("Failed to allocate second page: %v", err)
		}

		// 写入不同的数据
		testData2 := make([]byte, pageSize)
		for i := 0; i < pageSize; i++ {
			testData2[i] = byte((i + 1) % 256)
		}

		err = pager.WritePage(pageNum2, testData2)
		if err != nil {
			t.Errorf("Failed to write second page: %v", err)
		}

		// 读取并验证
		data2, err := pager.ReadPage(pageNum2)
		if err != nil {
			t.Errorf("Failed to read second page: %v", err)
		}

		if !bytes.Equal(data2, testData2) {
			t.Error("Read data from second page doesn't match written data")
		}
	})

	// 测试错误情况
	t.Run("Error Cases", func(t *testing.T) {
		// 测试读取不存在的页面
		_, err := pager.ReadPage(999)
		if err == nil {
			t.Error("Expected error when reading non-existent page")
		}

		// 测试写入错误大小的数据
		wrongSizeData := make([]byte, pageSize+1)
		err = pager.WritePage(0, wrongSizeData)
		if err == nil {
			t.Error("Expected error when writing wrong size data")
		}
	})

	// 测试同步到磁盘
	t.Run("Sync", func(t *testing.T) {
		err := pager.Sync()
		if err != nil {
			t.Errorf("Failed to sync to disk: %v", err)
		}
	})

	// 测试Getter方法
	t.Run("Getters", func(t *testing.T) {
		if pager.GetPageSize() != pageSize {
			t.Errorf("Expected page size %d, got %d", pageSize, pager.GetPageSize())
		}

		if pager.GetFileName() != filename {
			t.Errorf("Expected filename %s, got %s", filename, pager.GetFileName())
		}

		// totalPage应该是2，因为我们创建了两个页面
		if pager.GetTotalPage() != 2 {
			t.Errorf("Expected total pages 2, got %d", pager.GetTotalPage())
		}
	})
}
