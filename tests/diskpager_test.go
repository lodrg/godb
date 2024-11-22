package tests

import (
	"bytes"
	f "godb/file" // 导入你的包
	"os"
	"testing"
)

func TestDiskPager(t *testing.T) {
	// 创建临时文件
	tmpFile := "test_pager.db"
	defer os.Remove(tmpFile) // 测试完成后删除文件

	// 创建pager，设置页大小为100字节
	pager, err := f.NewDiskPager(tmpFile, 100)
	if err != nil {
		t.Fatalf("Failed to create pager: %v", err)
	}
	defer pager.Close()

	// 测试1: 分配新页面
	page0, err := pager.AllocateNewPage()
	if err != nil {
		t.Fatalf("Failed to allocate page 0: %v", err)
	}
	if page0 != 0 {
		t.Errorf("Expected page number 0, got %d", page0)
	}

	// 测试2: 再次分配，应该得到页面2
	page1, err := pager.AllocateNewPage()
	if err != nil {
		t.Fatalf("Failed to allocate page 1: %v", err)
	}
	if page1 != 1 {
		t.Errorf("Expected page number 1, got %d", page1)
	}

	// 测试3: 写入数据到页面0
	data0 := make([]byte, 100)
	copy(data0, []byte("Hello, Page 0!"))
	if err := pager.WritePage(page0, data0); err != nil {
		t.Fatalf("Failed to write to page 0: %v", err)
	}

	// 测试4: 写入数据到页面1
	data1 := make([]byte, 100)
	copy(data1, []byte("Hello, Page 1!"))
	if err := pager.WritePage(page1, data1); err != nil {
		t.Fatalf("Failed to write to page 1: %v", err)
	}

	// 测试5: 读取并验证页面0的数据
	readData0, err := pager.ReadPage(page0)
	if err != nil {
		t.Fatalf("Failed to read page 0: %v", err)
	}
	if !bytes.Equal(readData0[:14], []byte("Hello, Page 0!")) {
		t.Errorf("Page 0 data mismatch. Expected 'Hello, Page 0!', got '%s'", readData0[:13])
	}

	// 测试6: 读取并验证页面1的数据
	readData1, err := pager.ReadPage(page1)
	if err != nil {
		t.Fatalf("Failed to read page 1: %v", err)
	}
	if !bytes.Equal(readData1[:14], []byte("Hello, Page 1!")) {
		t.Errorf("Page 1 data mismatch. Expected 'Hello, Page 1!', got '%s'", readData1[:13])
	}

	// 测试7: 尝试读取不存在的页面
	_, err = pager.ReadPage(2)
	if err == nil {
		t.Error("Expected error when reading non-existent page")
	}

	// 测试8: 写入错误大小的数据
	invalidData := make([]byte, 50) // 页大小是100
	err = pager.WritePage(0, invalidData)
	if err == nil {
		t.Error("Expected error when writing invalid size data")
	}

	// 测试9: 验证总页数
	if pager.GetTotalPage() != 2 {
		t.Errorf("Expected total pages to be 2, got %d", pager.GetTotalPage())
	}
}
