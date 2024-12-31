package database

import (
	"encoding/json"
	"fmt"
	"godb/disktree"
	f "godb/file"
	"godb/sqlparser"
	"log"
	"os"
	"path/filepath"
	"strings"
)

// @Title        database.go
// @Description
// @Create       david 2024-12-31 15:25
// @Update       david 2024-12-31 15:25

type DataBase struct {
	dataDirectory    string
	tableDefinitions map[string]*SqlTableDefinition
	tableTrees       map[string]*disktree.BPTree
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
func NewWebpDataBase(dataDirectory string) *DataBase {
	db := &DataBase{
		dataDirectory:    dataDirectory,
		tableDefinitions: make(map[string]*SqlTableDefinition),
		tableTrees:       make(map[string]*disktree.BPTree),
	}

	// 读取表定义和初始化B+树
	db.tableDefinitions = db.readTableDefinition()
	db.tableTrees = db.initTableTrees()

	return db
}

func (b DataBase) readTableDefinition() map[string]*SqlTableDefinition {
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
			if err != nil {
				log.Fatal(err)
			}
			var table SqlTableDefinition
			err = json.Unmarshal(content, &table)
			if err != nil {
				fmt.Println("Error:", err)
			}
			tableDefinitions[table.tableName] = &table
		}
	}
	return tableDefinitions
}

func (b DataBase) initTableTrees() map[string]*disktree.BPTree {
	tableTrees := make(map[string]*disktree.BPTree)
	for tableName := range b.tableDefinitions {
		size := b.getRowSize(tableName)
		fileName := b.dataDirectory + ".db"
		diskPager, err := f.NewDiskPager(fileName, PAGE_SIZE, CACHE_SIZE)

		if err != nil {
			log.Fatal("Failed to allocate new page")
		}
		tree := disktree.NewBPTree(ORDER_SIZE, size, diskPager)
		tableTrees[tableName] = tree
	}
	return tableTrees
}

func (b DataBase) getRowSize(name string) uint32 {
	tableDefinition := b.tableDefinitions[name]
	rowSize := uint32(0)
	for _, column := range tableDefinition.columns {
		if column.dataType == sqlparser.INT {
			rowSize += INT_SIZE
		} else if column.dataType == sqlparser.CHAR {
			rowSize += CHAR_SIZE + CHAR_LENGTH
		} else {
			log.Fatal("Unknown column type:", column.dataType)
		}
	}
	return rowSize
}
