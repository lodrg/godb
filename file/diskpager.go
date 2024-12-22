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
	cacheSize int
	cache     map[int][]byte
	lru       *lru
	mu        sync.RWMutex // 添加互斥锁保护并发访问
}

func NewDiskPager(filename string, pageSize int, cacheSize int) (*DiskPager, error) {
	// 先删除已存在的文件
	if err := os.Remove(filename); err != nil && !os.IsNotExist(err) {
		// 如果删除失败且错误不是"文件不存在"，则返回错误
		return nil, fmt.Errorf("failed to remove existing file: %v", err)
	}

	// 创建新文件
	f, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to create file: %v", err)
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
	//fmt.Println("totalPage: ", totalPage)

	return &DiskPager{
		fileName:  filename,
		file:      f,
		pageSize:  pageSize,
		totalPage: totalPage,
		info:      info,
		cacheSize: cacheSize,
		cache:     make(map[int][]byte),
		lru:       newLRU(totalPage),
	}, nil
}

func (dp *DiskPager) addToCache(pageNum int, data []byte) {
	if len(dp.cache) >= dp.cacheSize {
		// remove the least used page
		lastUsed, b := dp.lru.removeLast()
		if b {
			delete(dp.cache, lastUsed)
		}
	}
	dp.cache[pageNum] = data
	//updatelru
	dp.lru.add(pageNum)
}

func (dp *DiskPager) ReadPage(pageNum int) ([]byte, error) {
	dp.mu.RLock()
	defer dp.mu.RUnlock()

	if pageNum > dp.totalPage {
		return nil, fmt.Errorf("page number %d out of range (total pages: %d)", pageNum, dp.totalPage)
	}

	// check cache first
	cachePage := dp.cache[pageNum]
	if cachePage != nil {
		//update lru
		dp.lru.add(pageNum)
		return cachePage, nil
	}

	pageData := make([]byte, dp.pageSize)

	if int64((pageNum+1)*dp.pageSize) > dp.info.Size() {
		return nil, fmt.Errorf("ErrPageOutOfRange: page %d extends beyond file length: %d", pageNum, dp.info.Size())
	}
	offset := int64(pageNum) * int64(dp.pageSize)
	n, err := dp.file.ReadAt(pageData, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read page: %w", err)
	}

	dp.addToCache(pageNum, pageData[:n])
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

	dp.addToCache(pageNum, data[:n])

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

	newPageNum := dp.totalPage
	dp.totalPage++

	newSize := int64(dp.totalPage) * int64(dp.pageSize)
	if err := dp.file.Truncate(newSize); err != nil {
		dp.totalPage--
		return 0, fmt.Errorf("failed to allocate new page: %w", err)
	}

	info, _ := dp.file.Stat()
	dp.info = info

	fmt.Printf("Allocating new page : %d\n", newPageNum)

	return newPageNum, nil
}

// Close 关闭文件
func (dp *DiskPager) Close() error {
	if dp.file != nil {
		return dp.file.Close()
	}
	if dp.lru != nil {
		return dp.lru.Close()
	}
	if dp.cache != nil {
		dp.cache = nil
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
