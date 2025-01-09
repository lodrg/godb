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
	//fmt.Printf("tableDefinitions: %v\n", db.tableDefinitions)
	// 每次都需要初始化吗？
	db.tableTrees = db.readTableTree()

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

func (b *DataBase) readTableTree() map[string]*disktree.BPTree {
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

func (b *DataBase) getRowSize(name string) uint32 {
	tableDefinition := b.tableDefinitions[name]
	rowSize := uint32(0)
	for _, column := range tableDefinition.Columns {
		//fmt.Printf("column name: %v \n", column.Name)
		//fmt.Printf("column type: %v \n", column.DataType)
		if column.DataType == sqlparser.INT {
			rowSize += INT_SIZE
		} else if column.DataType == sqlparser.CHAR {
			rowSize += CHAR_SIZE + CHAR_LENGTH
		} else {
			log.Fatal("Unknown column type:", column.DataType)
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

func (b *DataBase) processSelect(node sqlparser.SelectNode) map[string]interface{} {
	result := make(map[string]interface{}, 0)
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
	rows := b.getRows(tree, condition, tableDefinition)

	// rebuild result rows just return rows that you want
	columns := node.Columns
	for _, row := range rows {
		// 处理 SELECT * 的情况
		if len(columns) == 1 && columns[0].ColumnName == "*" {
			result = row
			break // 因为只需要一行数据，所以可以直接break
		}
		// 处理指定列
		for _, column := range columns {
			if value, exits := row[column.ColumnName]; exits {
				result[column.ColumnName] = value
			}
		}
	}
	return result
}

func (b *DataBase) processInsert(node sqlparser.InsertNode) uint32 {
	panic("TODO: processInsert")
}

func (b *DataBase) prcessCreateTable(node sqlparser.CreateTbaleNode) {
	panic("TODO: processCreate")
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

func (b *DataBase) getRows(tree *disktree.BPTree, condition *sqlparser.BinaryOpNode, definition *SqlTableDefinition) []map[string]interface{} {
	right := condition.Right.(*sqlparser.LiteralNode)
	priKey := right.Value.(uint32)
	all, _ := tree.SearchAll(priKey)

	if all != nil {
		rows := make([]map[string]interface{}, 0)
		for _, bytes := range all.([][]byte) {
			// deserialize
			rows = append(rows, deserializeRow(definition, bytes))
		}
		return rows
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
	columns := definition.Columns
	cur_position := 0
	for _, column := range columns {
		if column.DataType == sqlparser.INT {
			result[column.Name] = bytes[cur_position:INT_SIZE]
			cur_position += INT_SIZE
		} else if column.DataType == sqlparser.CHAR {
			result[column.Name] = bytes[cur_position : cur_position+CHAR_SIZE+CHAR_LENGTH]
			cur_position += CHAR_LENGTH + CHAR_LENGTH
		} else {
			log.Fatal("DeserializeRow Unknown column type:", column.DataType)
		}
	}
	return result
}

func getRowSize(definition *SqlTableDefinition) int {
	size := 0
	for _, column := range definition.Columns {
		if column.DataType == sqlparser.INT {
			size += INT_SIZE
		} else if column.DataType == sqlparser.CHAR {
			size += CHAR_SIZE + CHAR_LENGTH
		} else {
			log.Fatal("getRowSize Unknown column type:", column.DataType)
		}
	}
	return size
}

func getPriName(definition *SqlTableDefinition) (string, error) {
	for _, column := range definition.Columns {
		if column.IsPrimaryKey {
			return column.Name, nil // 返回列名而不是数据类型
		}
	}
	return "", fmt.Errorf("primary key not exist in table definition")
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
