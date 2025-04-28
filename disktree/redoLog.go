package disktree

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"godb/logger"
	"io"
	"os"
)

// @Title        redoLog.go
// @Description
// @Create       david 2025-02-10 14:44
// @Update       david 2025-02-10 14:44

type RedoLog struct {
	logFilePath               string
	logFile                   *os.File
	fileInfo                  os.FileInfo
	currentPosition           int32
	logSequenceNumber         int32
	executedLogSequenceMumber int32
	recovering                bool
	IsClosed                  bool
}

const (
	LOG_ENTRY_SIZE                     = 17
	HEADER_SIZE                        = 4
	INSERT_ROOT_NEW              int32 = 1
	INSERT_LEAF_SPLIT            int32 = 2
	INSERT_INTERNAL_SPLIT        int32 = 3
	INSERT_LEAF_NORMAL           int32 = 4
	INSERT_INTERNAL_NORMAL       int32 = 5
	LOG_SEQUENCE_NUMBER          int32 = 1
	EXECUTED_LOG_SEQUENCE_NUMBER int32 = 0
	LOG_METADATA_SIZE            int32 = 4
)

func NewRedoLog(filePath string) (*RedoLog, error) {
	// new file
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, fmt.Errorf("error initializing redo log: %w", err)
	}

	// get stat
	fileInfo, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("error getting file info: %w", err)
	}

	fileInfo.Size()

	rl := &RedoLog{
		logFilePath:               filePath,
		logFile:                   file,
		fileInfo:                  fileInfo,
		currentPosition:           LOG_METADATA_SIZE,
		logSequenceNumber:         LOG_SEQUENCE_NUMBER,
		executedLogSequenceMumber: EXECUTED_LOG_SEQUENCE_NUMBER,
		recovering:                false,
		IsClosed:                  false,
	}

	// 如果是新文件，初始化 header
	if fileInfo.Size() == 0 {
		if _, err := file.Seek(0, io.SeekStart); err != nil {
			file.Close()
			return nil, fmt.Errorf("error seeking to start position: %w", err)
		}
		err := rl.WriteInt(EXECUTED_LOG_SEQUENCE_NUMBER)
		if err != nil {
			file.Close()
			return nil, fmt.Errorf("error writing initial log: %w", err)
		}
		if err := file.Sync(); err != nil {
			file.Close()
			return nil, fmt.Errorf("error syncing file: %w", err)
		}
	} else {
		// 所以需要显式移动到正确位置
		if _, err := file.Seek(0, io.SeekStart); err != nil {
			file.Close()
			return nil, fmt.Errorf("error seeking to start position: %w", err)
		}
		exeLsn, err := rl.ReadInt()
		if err != nil {
			return nil, fmt.Errorf("error reading initial log: %w", err)
		}
		rl.executedLogSequenceMumber = exeLsn
	}

	_, err = file.Seek(0, io.SeekCurrent)
	//position, err := file.Seek(0, io.SeekCurrent)
	//fmt.Print(position)

	return rl, nil
}

/*
 * header format:
 * logSequenceNumber (4 bytes)
 * nextPosition (4 bytes)
 *
 */
func (l *RedoLog) logHeader(buffer *bytes.Buffer, capacity int32) (int32, error) {
	nextPosition := l.currentPosition + capacity
	binary.Write(buffer, binary.LittleEndian, l.logSequenceNumber)
	binary.Write(buffer, binary.LittleEndian, nextPosition)
	return nextPosition, nil
}

/*
 * INSERT_ROOT_NEW log format:
 * logSequenceNumber (4 bytes)
 * nextPosition (4 bytes)
 * operation (4 bytes)
 * key (4 bytes)
 * childPageNumber1 (4 bytes)
 * childPageNumber2 (4 bytes)
 */
func (l *RedoLog) LogInsertRootNew(key int32, childPageNum1 int32, childPageNum2 int32) (int32, error) {
	capacity := 4 * 6
	buffer := bytes.NewBuffer(make([]byte, 0, capacity))
	nextPosition, err := l.logHeader(buffer, int32(capacity))
	if err != nil {
		return 0, err
	}
	binary.Write(buffer, binary.LittleEndian, INSERT_ROOT_NEW)
	binary.Write(buffer, binary.LittleEndian, key)
	binary.Write(buffer, binary.LittleEndian, childPageNum1)
	binary.Write(buffer, binary.LittleEndian, childPageNum2)
	entry, err := l.writeLogEntry(buffer, int32(nextPosition))
	if err != nil {
		return 0, err
	}
	return entry, nil
}

func (l *RedoLog) RecoverInsertRootNew(tree *BPTree) {
	buffer := make([]byte, 4*3)
	l.logFile.Read(buffer)
	var key uint32
	var childPageNum1 uint32
	var childPageNum2 uint32
	binary.Read(bytes.NewBuffer(buffer), binary.LittleEndian, &key)
	binary.Read(bytes.NewBuffer(buffer), binary.LittleEndian, &childPageNum1)
	binary.Read(bytes.NewBuffer(buffer), binary.LittleEndian, &childPageNum2)
	tree.InsertRootNew(key, childPageNum1, childPageNum2)
}

/*
 * INSERT_LEAF_NORMAL log format:
 * logSequenceNumber (4 bytes)
 * nextPosition (4 bytes)
 * operation (4 bytes)
 * pageNumber (4 bytes)
 * newKey (4 bytes)
 * newValueLength (4 bytes)
 * newValue (variable length)
 */
func (l *RedoLog) LogInsertLeafNormal(pageNumber int32, newKey int32, newValue []byte) (int32, error) {
	capacity := 4*6 + len(newValue)
	buffer := bytes.NewBuffer(make([]byte, 0, capacity))
	nextPosition, err := l.logHeader(buffer, int32(capacity))
	if err != nil {
		return 0, err
	}
	binary.Write(buffer, binary.LittleEndian, INSERT_LEAF_NORMAL)
	binary.Write(buffer, binary.LittleEndian, pageNumber)
	binary.Write(buffer, binary.LittleEndian, newKey)
	binary.Write(buffer, binary.LittleEndian, len(newValue))
	binary.Write(buffer, binary.LittleEndian, newValue)
	entry, err := l.writeLogEntry(buffer, int32(nextPosition))
	if err != nil {
		return 0, err
	}
	return entry, nil
}

func (l *RedoLog) RecoverLogInsertLeafNormal(order uint32, pager *DiskPager) {
	buffer := make([]byte, 4*3)
	l.logFile.Read(buffer)
	var pageNumber uint32
	var newKey uint32
	var newValueLen uint32
	var newValue []byte
	binary.Read(bytes.NewBuffer(buffer), binary.LittleEndian, &pageNumber)
	binary.Read(bytes.NewBuffer(buffer), binary.LittleEndian, &newKey)
	binary.Read(bytes.NewBuffer(buffer), binary.LittleEndian, &newValueLen)
	binary.Read(bytes.NewBuffer(buffer), binary.LittleEndian, &newValue)
	disk := ReadDisk(order, pager, pageNumber, l).(*DiskLeafNode)
	disk.Insert(newKey, newValue)
}

/*
 * INSERT_INTERNAL_NORMAL log format:
 * logSequenceNumber (4 bytes)
 * nextPosition (4 bytes)
 * operation (4 bytes)
 * pageNumber (4 bytes)
 * newKey (4 bytes)
 * newChildPageNumber (4 bytes)
 */
func (l *RedoLog) LogInsertInternalNormal(pageNumber int32, newKey int32, newChildPageNumber int) (int32, error) {
	capacity := 4 * 6
	buffer := bytes.NewBuffer(make([]byte, 0, capacity))
	nextPosition, err := l.logHeader(buffer, int32(capacity))
	if err != nil {
		return 0, err
	}
	binary.Write(buffer, binary.LittleEndian, INSERT_INTERNAL_NORMAL)
	binary.Write(buffer, binary.LittleEndian, pageNumber)
	binary.Write(buffer, binary.LittleEndian, newKey)
	binary.Write(buffer, binary.LittleEndian, newChildPageNumber)
	entry, err := l.writeLogEntry(buffer, int32(nextPosition))
	if err != nil {
		return 0, err
	}
	return entry, nil
}

func (l *RedoLog) RecoverLogInsertInternalNormal(order uint32, pager *DiskPager) {
	buffer := make([]byte, 4*3)
	l.logFile.Read(buffer)
	var pageNumber uint32
	var newKey uint32
	var newChildPageNumber uint32
	binary.Read(bytes.NewBuffer(buffer), binary.LittleEndian, &pageNumber)
	binary.Read(bytes.NewBuffer(buffer), binary.LittleEndian, &newKey)
	binary.Read(bytes.NewBuffer(buffer), binary.LittleEndian, &newChildPageNumber)
	disk := ReadDisk(order, pager, pageNumber, l).(*DiskInternalNode)
	disk.insertIntoNode(newKey, newChildPageNumber)
}

/*
 * INSERT_LEAF_SPLIT log format:
 * logSequenceNumber (4 bytes)
 * nextPosition (4 bytes)
 * operation (4 bytes)
 * pageNumber (4 bytes)
 */
func (l *RedoLog) LogInsertLeafSplit(pageNumber int32) (int32, error) {
	capacity := 4 * 4
	buffer := bytes.NewBuffer(make([]byte, 0, capacity))
	nextPosition, err := l.logHeader(buffer, int32(capacity))
	if err != nil {
		return 0, err
	}
	binary.Write(buffer, binary.LittleEndian, INSERT_LEAF_SPLIT)
	binary.Write(buffer, binary.LittleEndian, pageNumber)
	entry, err := l.writeLogEntry(buffer, int32(nextPosition))
	if err != nil {
		return 0, err
	}
	return entry, nil
}

func (l *RedoLog) RecoverLogInsertLeafSplit(order uint32, pager *DiskPager) {
	buffer := make([]byte, 4*3)
	l.logFile.Read(buffer)
	var pageNumber uint32
	binary.Read(bytes.NewBuffer(buffer), binary.LittleEndian, &pageNumber)
	disk := ReadDisk(order, pager, pageNumber, l).(*DiskLeafNode)
	disk.split()
}

/*
 * INSERT_INTERNAL_SPLIT log format:
 * logSequenceNumber (4 bytes)
 * nextPosition (4 bytes)
 * operation (4 bytes)
 * pageNumber (4 bytes)
 */
func (l *RedoLog) LogInsertInternalSplit(pageNumber int32) (int32, error) {
	capacity := 4 * 4
	buffer := bytes.NewBuffer(make([]byte, 0, capacity))
	nextPosition, err := l.logHeader(buffer, int32(capacity))
	if err != nil {
		return 0, err
	}
	binary.Write(buffer, binary.LittleEndian, INSERT_INTERNAL_SPLIT)
	binary.Write(buffer, binary.LittleEndian, pageNumber)
	entry, err := l.writeLogEntry(buffer, int32(nextPosition))
	if err != nil {
		return 0, err
	}
	return entry, nil
}

func (l *RedoLog) RecoverLogInsertInternalSplit(order uint32, pager *DiskPager) {
	buffer := make([]byte, 4*3)
	l.logFile.Read(buffer)
	var pageNumber uint32
	binary.Read(bytes.NewBuffer(buffer), binary.LittleEndian, &pageNumber)
	disk := ReadDisk(order, pager, pageNumber, l).(*DiskInternalNode)
	disk.splitInternalNode()
}

func (l *RedoLog) writeLogEntry(buffer *bytes.Buffer, nextPosition int32) (int32, error) {
	if _, err := l.logFile.Seek(int64(l.currentPosition), io.SeekStart); err != nil {
		l.logFile.Close()
		return 0, fmt.Errorf("error seeking to start position: %w", err)
	}
	_, err := l.logFile.Write(buffer.Bytes())
	if err != nil {
		return 0, fmt.Errorf("error writing log: %w", err)
	}
	l.logFile.Sync()
	l.currentPosition = int32(nextPosition)
	if l.recovering {
		return -1, nil
	} else {
		oldLogSequenceNumber := l.logSequenceNumber
		l.logSequenceNumber++
		return oldLogSequenceNumber, nil
	}
}

// mark exec position is exec position
func (l *RedoLog) MarkExecuted(logSeguenceNumber int32) error {
	if logSeguenceNumber > l.executedLogSequenceMumber {
		l.executedLogSequenceMumber = logSeguenceNumber

		_, err := l.logFile.Seek(0, io.SeekStart)
		if err != nil {
			return fmt.Errorf("error seeking to start position: %w", err)
		}
		l.WriteInt(logSeguenceNumber)
		return l.logFile.Sync()
	}
	return nil
}

/*
 * header format:
 * logSequenceNumber (4 bytes)
 * nextPosition (4 bytes)
 * operation (4 bytes)
 *
 */
func (l *RedoLog) Recover(bpt *BPTree) error {
	l.recovering = true

	_, err := l.logFile.Seek(0, io.SeekStart)
	if err != nil {
		return fmt.Errorf("error seeking to start position: %w", err)
	}
	exeLogSeqNumber, err := l.ReadInt()
	l.executedLogSequenceMumber = exeLogSeqNumber
	l.currentPosition = LOG_METADATA_SIZE

	for l.currentPosition < int32(l.fileInfo.Size()) {
		_, err := l.logFile.Seek(int64(l.currentPosition), io.SeekStart)
		if err != nil {
			return fmt.Errorf("error seeking to start position: %w", err)
		}
		buffer := make([]byte, 4*3)
		l.logFile.Read(buffer)
		var logSequenceNumber int32
		var nextPosition int32
		var operation int32
		binary.Read(bytes.NewBuffer(buffer), binary.LittleEndian, &logSequenceNumber)
		binary.Read(bytes.NewBuffer(buffer), binary.LittleEndian, &nextPosition)
		binary.Read(bytes.NewBuffer(buffer), binary.LittleEndian, &operation)
		if logSequenceNumber > int32(l.executedLogSequenceMumber) {
			order := bpt.order
			pager := &bpt.DiskPager
			switch operation {
			case INSERT_ROOT_NEW:
				l.RecoverInsertRootNew(bpt)
				break
			case INSERT_LEAF_NORMAL:
				l.RecoverLogInsertLeafNormal(order, pager)
				break
			case INSERT_INTERNAL_NORMAL:
				l.RecoverLogInsertInternalNormal(order, pager)
				break
			case INSERT_LEAF_SPLIT:
				l.RecoverLogInsertLeafSplit(order, pager)
				break
			case INSERT_INTERNAL_SPLIT:
				l.RecoverLogInsertInternalSplit(order, pager)
				break
			}
			l.currentPosition = nextPosition
		}
	}

	l.recovering = false
	return nil
}

func (l *RedoLog) Close() {
	err := l.logFile.Close()
	if err != nil {
		logger.Error("error closing log file")
	}
}

func (l *RedoLog) Delete() {
	l.Close()
	err := os.Remove(l.logFile.Name())
	if err != nil {
		logger.Error("error deleting log file")
	}
	logger.Info("redolog have been deleted")
}

func (rl *RedoLog) WriteInt(n int32) error {
	// 写入固定大小的int32
	return binary.Write(rl.logFile, binary.LittleEndian, n)
}
func (rl *RedoLog) ReadInt() (int32, error) {
	buf := make([]byte, 4)
	_, err := rl.logFile.Read(buf)
	if err != nil {
		return 0, err
	}
	var value int32
	err = binary.Read(bytes.NewReader(buf), binary.LittleEndian, &value)
	return value, err
}
