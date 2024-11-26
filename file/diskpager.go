package file

import (
	"fmt"
	"os"
	"sync"
)

type DiskPager struct {
	fileName  string
	file      *os.File
	totalPage int
	pageSize  int
	info      os.FileInfo
	mu        sync.RWMutex // 添加互斥锁保护并发访问
}

func NewDiskPager(filename string, pageSize int) (*DiskPager, error) {
	// 打开文件
	f, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		println("can't open file")
		return nil, err
	}

	// 获取文件信息
	info, err := f.Stat()
	if err != nil {
		println("can't get file status")
		f.Close() // 发生错误时关闭文件
		return nil, err
	}

	// 计算总页数
	totalPage := int(info.Size()) / pageSize
	if info.Size()%int64(pageSize) > 0 {
		totalPage++
	}

	return &DiskPager{
		fileName:  filename,
		file:      f,
		pageSize:  pageSize,
		totalPage: totalPage,
		info:      info,
	}, nil
}

func (dp *DiskPager) ReadPage(pageNum int) ([]byte, error) {
	dp.mu.RLock()
	defer dp.mu.RUnlock()

	if pageNum > dp.totalPage {
		return nil, fmt.Errorf("page number %d out of range (total pages: %d)", pageNum, dp.totalPage)
	}
	pageData := make([]byte, dp.pageSize)

	if int64((pageNum+1)*dp.pageSize) > dp.info.Size() {
		return nil, fmt.Errorf("ErrPageOutOfRange: page %d extends beyond file length", pageNum)
	}
	offset := int64(pageNum) * int64(dp.pageSize)
	n, err := dp.file.ReadAt(pageData, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read page: %w", err)
	}

	return pageData[:n], nil
}

func (dp *DiskPager) WritePage(pageNum int, data []byte) error {
	dp.mu.Lock()
	defer dp.mu.Unlock()

	if dp.pageSize != len(data) {
		return fmt.Errorf("page is not in file length")
	}
	if pageNum > dp.totalPage {
		return fmt.Errorf("page number out of range")
	}
	offset := int64(pageNum) * int64(dp.pageSize)
	n, err := dp.file.WriteAt(data, offset)
	if err != nil {
		return fmt.Errorf("failed to read page: %w", err)
	}
	if n != len(data) {
		return fmt.Errorf("incomplete write: wrote %d bytes out of %d", n, len(data))
	}
	// 如果写入了新页面，更新文件信息
	if pageNum >= dp.totalPage {
		if err := dp.updateFileInfo(); err != nil {
			return fmt.Errorf("failed to update file info: %w", err)
		}
	}

	return nil
}

func (dp *DiskPager) updateFileInfo() error {
	info, err := dp.file.Stat()
	if err != nil {
		return err
	}
	dp.info = info
	dp.totalPage = int(info.Size()) / dp.pageSize
	if info.Size()%int64(dp.pageSize) > 0 {
		dp.totalPage++
	}
	return nil
}

// AllocateNewPage 分配新页面并返回页号
func (dp *DiskPager) AllocateNewPage() (int, error) {
	dp.mu.Lock()
	defer dp.mu.Unlock()

	// 获取当前页号
	newPageNum := dp.totalPage

	// 扩展文件大小到新页面
	newSize := int64(dp.totalPage+1) * int64(dp.pageSize)
	if err := dp.file.Truncate(newSize); err != nil {
		return 0, fmt.Errorf("failed to allocate new page: %w", err)
	}

	// 更新文件信息
	if err := dp.updateFileInfo(); err != nil {
		return 0, fmt.Errorf("failed to update file info: %w", err)
	}

	return newPageNum, nil
}

// Close 关闭文件
func (dp *DiskPager) Close() error {
	if dp.file != nil {
		return dp.file.Close()
	}
	return nil
}

// Sync 同步文件到磁盘
func (dp *DiskPager) Sync() error {
	dp.mu.Lock()
	defer dp.mu.Unlock()
	return dp.file.Sync()
}

// Getter 方法
func (dp *DiskPager) GetTotalPage() int {
	return dp.totalPage
}

func (dp *DiskPager) GetPageSize() int {
	return dp.pageSize
}

func (dp *DiskPager) GetFileName() string {
	return dp.fileName
}
