package file

import (
	"bytes"
	"fmt"
	"godb/logger"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type DiskPager struct {
	// 文件相关
	fileName  string
	file      *os.File
	totalPage atomic.Uint32
	pageSize  int
	info      os.FileInfo

	// 缓存相关
	cacheSize int
	cache     sync.Map
	lru       *lru

	// redolog
	logSequenceNumberMap sync.Map

	// dirty page
	dirtyPage  sync.Map
	wg         sync.WaitGroup
	shutdownCh chan struct{}

	mu sync.RWMutex // 添加互斥锁保护并发访问
}

const (
	FLASHiNTERVAL = 1000
)

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

	dp := &DiskPager{
		fileName:             filename,
		file:                 f,
		pageSize:             pageSize,
		info:                 info,
		cacheSize:            cacheSize,
		cache:                sync.Map{},
		dirtyPage:            sync.Map{},
		logSequenceNumberMap: sync.Map{},
		shutdownCh:           make(chan struct{}),
		lru:                  newLRU(totalPage),
	}
	dp.totalPage.Store(uint32(totalPage))

	dp.wg.Add(1)
	go dp.flushWorker()

	return dp, nil
}

func (dp *DiskPager) WritePage(pageNum int, data []byte, logSequenceNumber int32) error {
	dp.mu.Lock()
	defer dp.mu.Unlock()

	if dp.pageSize != len(data) {
		return fmt.Errorf("page is not in file length")
	}
	if uint32(pageNum) > dp.totalPage.Load() {
		return fmt.Errorf("page number out of range")
	}

	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)

	//offset := int64(pageNum) * int64(dp.pageSize)
	//n, err := dp.file.WriteAt(data, offset)
	//if err != nil {
	//	return fmt.Errorf("failed to read page: %w", err)
	//}

	dp.addToCache(pageNum, dataCopy)
	dp.addToDirtyPage(pageNum, dataCopy)
	dp.logSequenceNumberMap.Store(pageNum, logSequenceNumber)

	// 如果写入了新页面，更新文件信息
	if uint32(pageNum) >= dp.totalPage.Load() {
		if err := dp.updateFileInfo(); err != nil {
			return fmt.Errorf("failed to update file info: %w", err)
		}
	}

	return nil
}

func (dp *DiskPager) ReadPage(pageNum int) ([]byte, error) {
	dp.mu.RLock()
	defer dp.mu.RUnlock()

	if uint32(pageNum) > dp.totalPage.Load() {
		return nil, fmt.Errorf("page number %d out of range (total pages: %d)", pageNum, dp.totalPage)
	}

	// check cache first
	cachePage, _ := dp.cache.Load(pageNum)
	if cachePage != nil {
		//update lru
		dp.lru.add(pageNum)
		return cachePage.([]byte), nil
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

func (dp *DiskPager) addToCache(pageNum int, data []byte) {

	count := 0
	dp.cache.Range(func(key, value interface{}) bool {
		count++
		return true
	})

	for count >= dp.cacheSize {
		// remove the least used page
		lastUsed, b := dp.lru.removeLast()
		if b {
			dp.cache.Delete(lastUsed)
		}
	}
	dp.cache.Store(pageNum, data)
	//updatelru
	dp.lru.add(pageNum)
}

func (dp *DiskPager) addToDirtyPage(pageNum int, data []byte) {
	dp.dirtyPage.Store(pageNum, data)
}

func (dp *DiskPager) updateFileInfo() error {
	info, err := dp.file.Stat()
	if err != nil {
		return err
	}
	dp.info = info
	dp.totalPage.Store(uint32(info.Size()) / uint32(dp.pageSize))
	if info.Size()%int64(dp.pageSize) > 0 {
		dp.totalPage.Add(1)
	}
	return nil
}

// AllocateNewPage 分配新页面并返回页号
func (dp *DiskPager) AllocateNewPage() (int, error) {
	dp.mu.Lock()
	defer dp.mu.Unlock()

	newPageNum := dp.totalPage.Load()
	dp.totalPage.Add(1)

	newSize := int64(dp.totalPage.Load()) * int64(dp.pageSize)
	if err := dp.file.Truncate(newSize); err != nil {
		dp.totalPage.Add(^uint32(0))
		return 0, fmt.Errorf("failed to allocate new page: %w", err)
	}

	info, _ := dp.file.Stat()
	dp.info = info

	//fmt.Printf("Allocating new page : %d\n", newPageNum)
	logger.Info("total page now : %d \n", dp.totalPage)

	return int(newPageNum), nil
}

// Close 关闭文件
func (dp *DiskPager) Close() error {
	// 通知刷盘协程退出
	close(dp.shutdownCh)

	// 等待刷盘协程完成
	dp.wg.Wait()
	if dp.file != nil {
		return dp.file.Close()
	}
	if dp.lru != nil {
		return dp.lru.Close()
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
	return int(dp.totalPage.Load())
}

func (dp *DiskPager) GetPageSize() int {
	return dp.pageSize
}

func (dp *DiskPager) GetFileName() string {
	return dp.fileName
}

// 主动 flush
func (dp *DiskPager) Flush() error {
	dp.mu.Lock()
	defer dp.mu.Unlock()

	//for pageNum, data := range dp.dirtyPage.Range() {
	//	offset := int64(pageNum) * int64(dp.pageSize)
	//	n, err := dp.file.WriteAt(data, offset)
	//	if err != nil {
	//		return fmt.Errorf("failed to read page: %w", err)
	//	}
	//	if n != len(data) {
	//		return fmt.Errorf("failed to write page: expected to write %d bytes, wrote %d", len(data), n)
	//	}
	//	dp.dirtyPage.Delete(pageNum)
	//}
	dp.dirtyPage.Range(func(key, value interface{}) bool {
		pageNum := key.(int)   // 类型断言
		data := value.([]byte) // 类型断言

		offset := int64(pageNum) * int64(dp.pageSize)
		n, err := dp.file.WriteAt(data, offset)
		if err != nil {
			// 注意：Range的回调中不能直接return error
			// 可以通过闭包捕获外部的error变量
			return false // 停止遍历
		}
		if n != len(data) {
			return false // 停止遍历
		}

		dp.dirtyPage.Delete(pageNum)
		return true // 继续遍历
	})
	dp.file.Sync()
	return nil
}

// worker flush
func (dp *DiskPager) flushDirtyPages() {
	dp.mu.Lock()
	dirtyPages := make(map[int][]byte)
	//for pageNum, data := range dp.dirtyPage {
	//	dirtyPages[pageNum] = data
	//}
	dp.dirtyPage.Range(func(key, value interface{}) bool {
		pageNum := key.(int)
		data := value.([]byte) // 类型断言

		dirtyPages[pageNum] = data
		return true
	})
	dp.mu.Unlock()

	//for pageNum, data := range dp.dirtyPage {
	//	offset := int64(pageNum) * int64(dp.pageSize)
	//	n, err := dp.file.WriteAt(data, offset)
	//	if err != nil {
	//		continue
	//	}
	//	if n == len(data) {
	//		dp.mu.Lock()
	//		if bytes.Equal(dirtyPages[pageNum], data) {
	//			dp.dirtyPage.Delete(pageNum)
	//		}
	//		dp.mu.Unlock()
	//	}
	//}

	dp.dirtyPage.Range(func(key, value interface{}) bool {
		pageNum := key.(int)
		data := value.([]byte)

		offset := int64(pageNum) * int64(dp.pageSize)
		n, err := dp.file.WriteAt(data, offset)
		if err != nil {
			return false
		}
		if n == len(data) {
			dp.mu.Lock()
			if bytes.Equal(dirtyPages[pageNum], data) {
				dp.dirtyPage.Delete(pageNum)
			}
			dp.mu.Unlock()
		}
		return true
	})
	dp.file.Sync()
}

func (dp *DiskPager) flushWorker() {
	defer dp.wg.Done()

	ticker := time.NewTicker(time.Second * FLASHiNTERVAL)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			dp.flushDirtyPages()
		case <-dp.shutdownCh:
			dp.flushDirtyPages() // 最终刷盘
			return
		}
	}
}
