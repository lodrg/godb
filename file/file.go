package file

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"godb/logger"
	"io"
	"os"
)

const (
	IdLength     = 4
	NameLength   = 12
	RecordLength = IdLength + NameLength
)

type Record struct {
	ID   int32
	Name string
}

type SimpleDB struct {
	filename string
}

func NewSimpleDB(filename string) *SimpleDB {
	return &SimpleDB{filename: filename}
}

func (db *SimpleDB) Insert(id int32, name string) error {
	f, err := os.OpenFile(db.filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	buf := make([]byte, RecordLength)

	binary.BigEndian.PutUint32(buf[:IdLength], uint32(id))

	nameBytes := []byte(name)
	if len(nameBytes) > NameLength {
		nameBytes = nameBytes[:NameLength]
	}
	copy(buf[IdLength:], nameBytes)

	_, err = f.Write(buf)
	return err
}

func (db *SimpleDB) Select(idQuery int32) (*Record, error) {
	f, err := os.OpenFile(db.filename, os.O_APPEND|os.O_CREATE|os.O_RDONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer f.Close()

	idBuf := make([]byte, IdLength)
	nameBuf := make([]byte, NameLength)

	for {
		_, err := io.ReadFull(f, idBuf)
		if err != nil {
			if err == io.EOF {
				return nil, fmt.Errorf("eof: %w", err)
			}
			return nil, fmt.Errorf("failed to read id: %w", err)
		}

		_, err = io.ReadFull(f, nameBuf)
		if err != nil {
			return nil, fmt.Errorf("failed to read name: %w", err)
		}

		id := int32(binary.BigEndian.Uint32(idBuf))
		logger.Debug("id: %d\n", id)

		if id == idQuery {
			name := string(bytes.TrimRight(nameBuf, "\x00"))
			return &Record{
				ID:   id,
				Name: name,
			}, nil
		}
	}
}
