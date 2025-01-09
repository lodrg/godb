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
	// 每次都需要初始化吗？
	db.tableTrees = db.initTableTrees()

	return db
}

func (b *DataBase) readTableDefinition() map[string]*SqlTableDefinition {
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

func (b *DataBase) initTableTrees() map[string]*disktree.BPTree {
	tableTrees := make(map[string]*disktree.BPTree)
	for tableName := range b.tableDefinitions {
		size := b.getRowSize(tableName)
		fileName := b.dataDirectory + tableName + ".db"
		diskPager, err := f.NewDiskPager(fileName, PAGE_SIZE, CACHE_SIZE)

		if err != nil {
			log.Fatal("Failed to allocate new page")
		}
		tree := disktree.NewBPTree(ORDER_SIZE, size, diskPager)
		tableTrees[tableName] = tree
	}
	return tableTrees
}

func (b *DataBase) getRowSize(name string) uint32 {
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

func (b *DataBase) Execute(sql string) (ExecuteResult, error) {
	ASTNode, err := sqlparser.Parse(sql)
	if err != nil {
		log.Fatal(err)
	}
	switch Node := ASTNode.(type) {
	case sqlparser.SelectNode:
		result := b.processSelect(Node)
		return ForSelect(result, b.tableDefinitions[Node.TableName], &ASTNode), nil
	case sqlparser.InsertNode:
		affectedrows := b.processInsert(Node)
		return ForInsert(affectedrows, b.tableDefinitions[Node.TableName]), nil
	case sqlparser.CreateTbaleNode:
		b.prcessCreateTable(Node)
		return ForCreate(b.tableDefinitions[Node.TableName]), nil
	default:
		err := fmt.Errorf("Unknown node type: %T", ASTNode)
		return ForError(err.Error()), err
	}
}

func (b *DataBase) processSelect(node sqlparser.SelectNode) *[][]any {
	result := make([][]any, 0)
	// get table def form json
	tableDefinition := b.tableDefinitions[node.TableName]
	// get tree
	tree := b.tableTrees[node.TableName]

	if node.WhereClause == nil || len(node.WhereClause) == 0 {
		log.Fatal("Where clause is empty")
	}
	// find where from table def, and hit pk
	condition, _ := b.getPrimeryKeyCondition(node.WhereClause, tableDefinition)

	// use pk condition get data from tree
	rows := *b.getRows(tree, condition, tableDefinition)

	// rebuild result rows just return rows that you want
	columns := node.Columns
	for _, row :=  range rows {
		for _, column := range columns {
			if column.ColumnName == row
		}
	}
	return &result
}

func (b *DataBase) processInsert(node sqlparser.InsertNode) uint32 {

}

func (b *DataBase) prcessCreateTable(node sqlparser.CreateTbaleNode) {

}

func (b *DataBase) getPrimeryKeyCondition(clause []*sqlparser.BinaryOpNode, definition *SqlTableDefinition) (*sqlparser.BinaryOpNode, error) {
	priKeyName, err := getPriName(definition)
	if err != nil {
		log.Fatal(err)
	}
	for _, node := range clause {
		left := node.Left.(sqlparser.ColumnNode)
		if left.ColumnName == priKeyName {
			return node, nil
		}
	}
	return nil, fmt.Errorf("No primary key column found")
}

func (b *DataBase) getRows(tree *disktree.BPTree, condition *sqlparser.BinaryOpNode, definition *SqlTableDefinition) *[]map[string]interface{} {
	right := condition.Right.(*sqlparser.LiteralNode)
	priKey := right.Value.(uint32)
	all, _ := tree.SearchAll(priKey)

	if all != nil {
		rows := make([]map[string]interface{}, 0)
		for _, bytes := range all.([][]byte) {
			// deserialize
			rows = append(rows, deserializeRow(definition, bytes))
		}
		return &rows
	}
	log.Fatal("can't found data")
	return nil
}

func deserializeRow(definition *SqlTableDefinition, bytes []byte) map[string]interface{} {
	// check row size
	rowSize := getRowSize(definition)
	if rowSize < len(bytes) {
		log.Fatalf("Row size mismatch, row size: %d, expected row size: %d", len(bytes), rowSize)
	}

	// from bytes to typed date (use map here)
	result := make(map[string]interface{})
	columns := definition.columns
	cur_position := 0
	for _, column := range columns {
		if column.dataType == sqlparser.INT {
			result[column.name] = bytes[cur_position:INT_SIZE]
			cur_position += INT_SIZE
		} else if column.dataType == sqlparser.CHAR {
			result[column.name] = bytes[cur_position : cur_position+CHAR_SIZE+CHAR_LENGTH]
			cur_position += CHAR_LENGTH + CHAR_LENGTH
		} else {
			log.Fatal("Unknown column type:", column.dataType)
		}
	}
	return result
}

func getRowSize(definition *SqlTableDefinition) int {
	size := 0
	for _, column := range definition.columns {
		if column.dataType == sqlparser.INT {
			size += INT_SIZE
		} else if column.dataType == sqlparser.CHAR {
			size += CHAR_SIZE + CHAR_LENGTH
		} else {
			log.Fatal("Unknown column type:", column.dataType)
		}
	}
	return size
}

func getPriName(definition *SqlTableDefinition) (string, error) {
	for _, column := range definition.columns {
		if column.isPrimaryKey {
			return column.name, nil // 返回列名而不是数据类型
		}
	}
	return "", fmt.Errorf("primary key not exist in table definition")
}
