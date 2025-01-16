package database

import (
	"encoding/json"
	"fmt"
	"godb/disktree"
	f "godb/file"
	. "godb/sqlparser"
	"log"
	"os"
	"path/filepath"
	"strings"
)

// @Title        sqlTableManager.go
// @Description
// @Create       david 2025-01-15 14:26
// @Update       david 2025-01-15 14:26

type SqlTableManager struct {
	dataDirectory     string
	tableDefinitions  map[string]*SqlTableDefinition
	tablePrimaryIndex map[string]*disktree.BPTree
}

const (
	PAGE_SIZE   = 1024
	ORDER_SIZE  = 10
	INT_SIZE    = 4
	CHAR_LENGTH = 8
	CHAR_SIZE   = 4
	CACHE_SIZE  = 10
)

// 构造函数
func NewSqlTableManager(dataDirectory string) *SqlTableManager {
	stm := &SqlTableManager{
		dataDirectory:     dataDirectory,
		tableDefinitions:  make(map[string]*SqlTableDefinition),
		tablePrimaryIndex: make(map[string]*disktree.BPTree),
	}

	// 读取表定义和初始化B+树
	stm.tableDefinitions = stm.readTableDefinition()
	//fmt.Printf("tableDefinitions: %v\n", db.tableDefinitions)
	// 每次都需要初始化吗？
	stm.tablePrimaryIndex = stm.readTableTree()

	return stm
}

func (b *SqlTableManager) getTableDefinition(tableName string) *SqlTableDefinition {
	return b.tableDefinitions[tableName]
}

func (b *SqlTableManager) readTableDefinition() map[string]*SqlTableDefinition {
	if _, err := os.Stat(b.dataDirectory); os.IsNotExist(err) {
		err := os.MkdirAll(b.dataDirectory, 0755)
		if err != nil {
			log.Fatal(err)
		}
	}

	tableDefinitions := make(map[string]*SqlTableDefinition)

	dir, err := os.ReadDir(b.dataDirectory)
	if err != nil {
		log.Fatal(err)
	}
	for _, file := range dir {
		if info, _ := file.Info(); info.Mode().IsRegular() && strings.HasSuffix(file.Name(), ".json") {
			filePath := filepath.Join(b.dataDirectory, file.Name())
			content, err := os.ReadFile(filePath)
			//fmt.Printf("content: %s \n", content)
			if err != nil {
				log.Fatal(err)
			}
			var table SqlTableDefinition
			err = json.Unmarshal(content, &table)
			if err != nil {
				fmt.Println("Error:", err)
			}
			//fmt.Printf("json : %v \n", table)
			tableDefinitions[table.TableName] = &table
		}
	}
	return tableDefinitions
}

func (b *SqlTableManager) readTableTree() map[string]*disktree.BPTree {
	tableTrees := make(map[string]*disktree.BPTree)
	for tableName := range b.tableDefinitions {
		//fmt.Printf("tableName: %s \n", tableName)
		size := b.getRowSize(tableName)
		fileName := b.dataDirectory + "/" + tableName + ".db"
		diskPager, err := f.NewDiskPager(fileName, PAGE_SIZE, CACHE_SIZE)

		if err != nil {
			log.Fatal("Failed to allocate new page")
		}
		tree := disktree.NewBPTree(ORDER_SIZE, size, diskPager)
		tableTrees[tableName] = tree
	}
	return tableTrees
}

func (b *SqlTableManager) getRowSize(name string) uint32 {
	tableDefinition := b.tableDefinitions[name]
	rowSize := uint32(0)
	for _, column := range tableDefinition.Columns {
		//fmt.Printf("column name: %v \n", column.Name)
		//fmt.Printf("column type: %v \n", column.DataType)
		if column.DataType == TypeInt {
			rowSize += INT_SIZE
		} else if column.DataType == TypeChar {
			rowSize += CHAR_SIZE + CHAR_LENGTH
		} else {
			log.Fatal("Unknown column type:", column.DataType)
		}
	}
	return rowSize
}

func resetDataDirectory(dataDirectory string) error {
	// 检查目录是否存在
	if _, err := os.Stat(dataDirectory); err == nil {
		// 目录存在，读取目录中的所有文件
		files, err := os.ReadDir(dataDirectory)
		if err != nil {
			return fmt.Errorf("failed to read directory: %w", err)
		}

		// 删除所有文件
		for _, file := range files {
			if !file.IsDir() { // 只删除文件，不删除子目录
				err := os.Remove(filepath.Join(dataDirectory, file.Name()))
				if err != nil {
					return fmt.Errorf("failed to delete file %s: %w", file.Name(), err)
				}
			}
		}
	} else if os.IsNotExist(err) {
		// 目录不存在，创建目录
		err := os.MkdirAll(dataDirectory, 0755)
		if err != nil {
			return fmt.Errorf("failed to create directory: %w", err)
		}
	} else {
		// 其他错误
		return fmt.Errorf("failed to check directory: %w", err)
	}

	return nil
}

func (b *SqlTableManager) Close() {
	for _, tree := range b.tablePrimaryIndex {
		tree.DiskPager.Close()
	}
}
