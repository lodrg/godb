package database

import (
	"encoding/json"
	"fmt"
	"godb/disktree"
	f "godb/file"
	"godb/logger"
	. "godb/sqlparser"
	"log"
	"os"
	"path/filepath"
	"strconv"
)

// @Title        sqlQueryExecutor.go
// @Description
// @Create       david 2025-01-15 14:22
// @Update       david 2025-01-15 14:22

type SqlQueryExecutor struct {
	SqlTableManager *SqlTableManager
}

func NewSqlQueryExecutor(manager *SqlTableManager) *SqlQueryExecutor {
	return &SqlQueryExecutor{
		SqlTableManager: manager,
	}
}

func (e *SqlQueryExecutor) processSelect(node *SelectNode, tableDefinitions []*SqlTableDefinition) map[string]interface{} {
	result := make(map[string]interface{}, 0)
	// get table def from json
	tableDefinition := e.SqlTableManager.getTableDefinition(node.TableName)
	// get tree
	tree := e.SqlTableManager.tablePrimaryIndex[node.TableName]

	if node.WhereClause == nil || len(node.WhereClause) == 0 {
		log.Fatal("Where clause is empty")
	}
	// find where from table def, and hit pk
	condition, _ := getPrimeryKeyCondition(node.WhereClause, tableDefinition)

	// use pk condition get data from tree
	rows := GetRows(tree, condition, tableDefinition)

	//fmt.Printf("rows: %v \n", rows)
	logger.Debug("rows: %v \n", rows)
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
	//fmt.Printf("result: %v\n", result)
	logger.Debug("result: %v\n", result)
	return result
}

func (e *SqlQueryExecutor) processInsert(node *InsertNode, tableDefinitions []*SqlTableDefinition) uint32 {
	tableDef := e.SqlTableManager.getTableDefinition(node.TableName)
	tree := e.SqlTableManager.tablePrimaryIndex[node.TableName]

	// 格式化并验证值
	values := formatInsertValues(node, tableDef)

	// 获取并验证主键
	key := getPrimaryKey(values, tableDef)

	// 检查主键是否存在
	if _, exists := tree.Search(key); exists {
		log.Fatal("duplicate primary key found")
	}

	// 序列化并插入记录
	bufRecord := serializeRow(values, tableDef)
	return tree.Insert(key, bufRecord.Bytes())
}

func formatInsertValues(node *InsertNode, tableDef *SqlTableDefinition) map[string]interface{} {
	values := make(map[string]interface{})

	if len(node.Columns) == 0 {
		if len(node.Values) != len(tableDef.Columns) {
			panic(fmt.Sprintf("value count (%d) doesn't match column count (%d)",
				len(node.Values), len(tableDef.Columns)))
		}
		for i, col := range tableDef.Columns {
			values[col.Name] = node.Values[i]
		}
		return values
	}

	if len(node.Values) != len(node.Columns) {
		panic(fmt.Sprintf("value count (%d) doesn't match column count (%d)",
			len(node.Values), len(node.Columns)))
	}

	for i, colName := range node.Columns {
		values[colName] = node.Values[i]
	}

	for _, col := range tableDef.Columns {
		if _, exists := values[col.Name]; !exists {
			panic(fmt.Sprintf("missing value for column %s", col.Name))
		}
	}

	return values
}

func getPrimaryKey(values map[string]interface{}, tableDef *SqlTableDefinition) uint32 {
	for _, col := range tableDef.Columns {
		if !col.IsPrimaryKey {
			continue
		}

		val, exists := values[col.Name]
		if !exists {
			panic(fmt.Sprintf("missing value for primary key column %s", col.Name))
		}

		switch col.DataType {
		case TypeInt:
			if intVal, ok := val.(uint32); ok {
				return intVal
			}
			panic(fmt.Sprintf("invalid type for primary key column %s, expected uint32", col.Name))

		case TypeChar:
			strVal, ok := val.(string)
			if !ok {
				panic(fmt.Sprintf("invalid type for primary key column %s, expected string", col.Name))
			}
			key, err := strconv.ParseUint(strVal, 10, 32)
			if err != nil {
				panic(fmt.Sprintf("invalid primary key value: %s (must be a positive integer <= %d)",
					strVal, uint32(^uint32(0))))
			}
			return uint32(key)

		default:
			panic(fmt.Sprintf("unsupported primary key type for column %s", col.Name))
		}
	}

	panic("no primary key column found in table definition")
}

func (e *SqlQueryExecutor) prcessCreateTable(node *CreateTableNode, tableDefinitions []*SqlTableDefinition) (*SqlTableDefinition, error) {
	// create table definition
	definition := NewSqlTableDefinition(node.TableName, node.Columns)

	// create table directory
	if _, err := os.Stat(e.SqlTableManager.dataDirectory); os.IsNotExist(err) {
		err := os.MkdirAll(e.SqlTableManager.dataDirectory, 0755)
		if err != nil {
			return nil, fmt.Errorf("failed to create directory: %w", err)
		}
	}

	// ser(json) table definition
	jsonTableDef, err := json.Marshal(definition)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal definition: %w", err)
	}

	// json file
	jsonfilename := filepath.Join(e.SqlTableManager.dataDirectory, node.TableName+".json")
	err = os.WriteFile(jsonfilename, jsonTableDef, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to create json table: %w", err)
	}

	// init tree
	size := e.SqlTableManager.getRowSize(node.TableName)
	fileName := filepath.Join(e.SqlTableManager.dataDirectory, node.TableName+".db")
	diskPager, err := f.NewDiskPager(fileName, PAGE_SIZE, CACHE_SIZE)

	if err != nil {
		log.Fatal("Failed to allocate new page")
	}
	tree := disktree.NewBPTree(ORDER_SIZE, size, diskPager)
	e.SqlTableManager.tablePrimaryIndex[node.TableName] = tree

	// return table definition
	return definition, nil
}

func getPrimeryKeyCondition(clause []*BinaryOpNode, definition *SqlTableDefinition) (*BinaryOpNode, error) {
	priKeyName, err := getPriName(definition)
	if err != nil {
		log.Fatal(err)
	}
	for _, node := range clause {
		if left, ok := node.Left.(*ColumnNode); ok {
			if left.ColumnName == priKeyName {
				return node, nil
			}
		}
	}
	return nil, fmt.Errorf("No primary key column found")
}

func GetRows(tree *disktree.BPTree, condition *BinaryOpNode, definition *SqlTableDefinition) []map[string]interface{} {
	right := condition.Right.(*LiteralNode)
	priKey := right.Value.(uint32)
	all, _ := tree.SearchAll(priKey)

	if all != nil {
		rows := make([]map[string]interface{}, 0)
		for _, bytes := range all.([][]byte) {
			// deserialize
			rows = append(rows, deserializeRow(definition, bytes))
		}
		logger.Debug("rows: %v \n", rows)
		return rows
	}
	log.Fatal("can't found data")
	return nil
}

func getRowSize(definition *SqlTableDefinition) int {
	size := 0
	for _, column := range definition.Columns {
		if column.DataType == TypeInt {
			size += INT_SIZE
		} else if column.DataType == TypeChar {
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
