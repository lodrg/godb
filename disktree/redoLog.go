package disktree

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
)

// @Title        redoLog.go
// @Description
// @Create       david 2025-02-10 14:44
// @Update       david 2025-02-10 14:44

type RedoLog struct {
	logFilePath  string
	logFile      *os.File
	lastPosition int64
}

const (
	LOG_ENTRY_SIZE = 17
	HEADER_SIZE    = 4
)

// updateLastPosition 更新最后写入位置
func (l *RedoLog) updateLastPosition(position int64) error {
	positionBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(positionBytes, uint32(position))
	_, err := l.logFile.WriteAt(positionBytes, 0)
	if err != nil {
		return fmt.Errorf("error updating last position: %w", err)
	}
	l.lastPosition = position
	return l.logFile.Sync()
}

// readLastPosition 读取最后写入位置
func (l *RedoLog) readLastPosition() (int64, error) {
	positionBytes := make([]byte, 4)
	_, err := l.logFile.ReadAt(positionBytes, 0)
	if err != nil {
		return 0, fmt.Errorf("error reading last position: %w", err)
	}
	return int64(binary.BigEndian.Uint32(positionBytes)), nil
}

func (l RedoLog) LogInsert(pageNumber uint32, key uint32, value []byte) (int64, error) {
	// insert file
	// format : 1(executed) + 4(operator is insert) + 4(pageNumber) + 4(key) + 4(valueLen)
	headerSize := LOG_ENTRY_SIZE
	buffer := make([]byte, headerSize+len(value))

	buffer[0] = 0
	binary.LittleEndian.PutUint32(buffer[1:], uint32(pageNumber))
	binary.LittleEndian.PutUint32(buffer[5:], uint32(1))
	binary.LittleEndian.PutUint32(buffer[9:], uint32(key))
	binary.LittleEndian.PutUint32(buffer[13:], uint32(len(value)))

	copy(buffer[headerSize:], value)

	position, err := l.logFile.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, err
	}

	if _, err := l.logFile.Write(buffer); err != nil {
		return 0, err
	}

	err = l.logFile.Sync()
	if err != nil {
		return 0, err
	}

	return position, l.logFile.Sync()
}

func (l RedoLog) MarkExecuted(position int64) error {
	_, err := l.logFile.WriteAt([]byte{1}, position)
	if err != nil {
		log.Fatal(err)
	}
	return l.logFile.Sync()
}

func (l *RedoLog) Recover(bpt *BPTree) error {
	startPosition, err := l.readLastPosition()
	if err != nil {
		return fmt.Errorf("error reading last position: %w", err)
	}

	_, err = l.logFile.Seek(startPosition, io.SeekStart)
	if err != nil {
		return fmt.Errorf("error seeking to start position: %w", err)
	}

	buffer := make([]byte, LOG_ENTRY_SIZE)
	for {
		n, err := l.logFile.Read(buffer)
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("error reading log entry: %w", err)
		}
		if n != LOG_ENTRY_SIZE {
			break
		}

		entryPosition := startPosition
		startPosition += int64(LOG_ENTRY_SIZE)

		executed := buffer[0]
		operation := binary.LittleEndian.Uint32(buffer[1:5])
		binary.LittleEndian.Uint32(buffer[5:9])
		key := binary.LittleEndian.Uint32(buffer[9:13])
		valueLength := binary.LittleEndian.Uint32(buffer[13:17])

		if executed == 0 && operation == 1 {
			valueBuffer := make([]byte, valueLength)
			_, err := l.logFile.Read(valueBuffer)
			if err != nil {
				return fmt.Errorf("error reading value: %w", err)
			}
			startPosition += int64(valueLength)

			if num := bpt.Insert(key, valueBuffer); num != 1 {
				return fmt.Errorf("error inserting into tree")
			}

			if err := l.MarkExecuted(entryPosition); err != nil {
				return err
			}
		} else {
			// 跳过 value
			startPosition += int64(valueLength)
			_, err := l.logFile.Seek(int64(valueLength), io.SeekCurrent)
			if err != nil {
				return fmt.Errorf("error seeking past value: %w", err)
			}
		}
	}
	return nil
}

func NewRedoLog(filePath string) (*RedoLog, error) {
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, fmt.Errorf("error initializing redo log: %w", err)
	}

	fileInfo, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("error getting file info: %w", err)
	}

	rl := &RedoLog{
		logFilePath: filePath,
		logFile:     file,
	}

	// 如果是新文件，初始化 header
	if fileInfo.Size() == 0 {
		if err := rl.updateLastPosition(int64(HEADER_SIZE)); err != nil {
			file.Close()
			return nil, fmt.Errorf("error initializing header: %w", err)
		}
	}

	// 读取上次位置
	lastPosition, err := rl.readLastPosition()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("error reading last position: %w", err)
	}
	rl.lastPosition = lastPosition

	return rl, nil
}

func (r *RedoLog) GetCurrentPosition() (int64, error) {
	// 直接获取当前位置
	return r.logFile.Seek(0, io.SeekCurrent)
}

func (r *RedoLog) GetFileSize() (int64, error) {
	// 获取文件大小
	return r.logFile.Seek(0, io.SeekEnd)
}
