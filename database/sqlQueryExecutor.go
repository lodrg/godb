package database

import (
	"fmt"
	"godb/disktree"
	. "godb/entity"
	"godb/logger"
	"log"
	"slices"
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
	logger.Debug("start process select sql")
	result := make(map[string]interface{}, 0)
	tableDefinition := e.SqlTableManager.getTableDefinition(node.TableName)
	primaryTree := e.SqlTableManager.tablePrimaryIndex[node.TableName]
	indexUsed := false

	if node.WhereClause == nil || len(node.WhereClause) == 0 {
		log.Fatal("Where clause is empty")
	}
	//priKey := statmentPrimaryKey(node, tableDefinition)
	// primary key "="
	condition, err := getPrimeryKeyCondition(node.WhereClause, tableDefinition, EQUALS)
	if err == nil {
		priKey := condition.Right.(*LiteralNode).Value.(uint32)
		rows := GetPrimaryTreeRows(primaryTree, priKey, tableDefinition)
		result = trimRows(node, rows, result)
		indexUsed = true
	}

	// secondary key "="
	if !indexUsed {
		condition, err := getSecondaryKeyCondition(node.WhereClause, tableDefinition, EQUALS)
		left := condition.Left.(*ColumnNode)
		secondaryTree := e.SqlTableManager.getSecondaryIndex(node.TableName, left.ColumnName)
		if err == nil {
			secondaryIndex := condition.Right.(*LiteralNode).Value.(uint32)
			// TODO
			rows := GetSecondaryTreeRowsFromPri(secondaryTree, secondaryIndex, primaryTree, tableDefinition)
			result = trimRows(node, rows, result)
		}
		indexUsed = true
	}

	// primary key "in"
	if !indexUsed {
		condition, err := getPrimeryKeyCondition(node.WhereClause, tableDefinition, IN)
		if err == nil {
			priKey := condition.Right.(*LiteralNode).Value.(uint32)
			rows := GetPrimaryTreeRows(primaryTree, priKey, tableDefinition)
			result = trimRows(node, rows, result)
		}
		indexUsed = true
	}

	// secondary key "="
	if !indexUsed {
		condition, err := getSecondaryKeyCondition(node.WhereClause, tableDefinition, IN)
		left := condition.Left.(*ColumnNode)
		secondaryTree := e.SqlTableManager.getSecondaryIndex(node.TableName, left.ColumnName)
		if err == nil {
			priKey := condition.Right.(*LiteralNode).Value.(uint32)
			rows := GetSecondaryTreeRows(secondaryTree, priKey, tableDefinition)
			result = trimRows(node, rows, result)
		}
		indexUsed = true
	}

	return result
}

func (e *SqlQueryExecutor) processInsert(node *InsertNode, tableDefinitions []*SqlTableDefinition) uint32 {
	logger.Debug("start process insert sql")
	tableDef := e.SqlTableManager.getTableDefinition(node.TableName)
	tree := e.SqlTableManager.tablePrimaryIndex[node.TableName]

	// 格式化并验证值
	values := formatInsertValues(node, tableDef)

	// 获取并验证主键
	key := checkPrimaryKeyExisting(values, tableDef, tree)

	// 序列化并插入记录
	bufRecord := serializeRow(values, tableDef)
	tree.Insert(key, bufRecord.Bytes())

	// secondary indexes
	e.insertIntoSecondaryIndex(node, tableDef, key)

	return 1
}
func (e *SqlQueryExecutor) prcessCreateTable(node *CreateTableNode, tableDefinitions []*SqlTableDefinition) (*SqlTableDefinition, error) {
	logger.Debug("start process create table sql")
	// create table definition
	definition := NewSqlTableDefinition(node.TableName, node.Columns)
	for _, column := range definition.Columns {
		if column.IndexType != None {
			if column.DataType != TypeInt {
				return nil, fmt.Errorf("index can only be created on numeric columns")
			}
		}
	}
	e.SqlTableManager.addAndPersistTableDefinition(definition)
	e.SqlTableManager.addPrimaryIndex(definition)
	e.SqlTableManager.addSecondaryIndex(definition)

	// return table definition
	return definition, nil
}

func trimRows(node *SelectNode, rows []map[string]interface{}, result map[string]interface{}) map[string]interface{} {
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

func checkPrimaryKeyExisting(values map[string]interface{}, tableDef *SqlTableDefinition, tree *disktree.BPTree) uint32 {
	key := getPrimaryKey(values, tableDef)

	// 检查主键是否存在
	if _, exists := tree.Search(key); exists {
		log.Fatal("duplicate primary key found")
	}

	return key
}

func (e *SqlQueryExecutor) insertIntoSecondaryIndex(node *InsertNode, tableDef *SqlTableDefinition, key uint32) {
	inedxes := e.SqlTableManager.getTableIndexes(node.TableName)

	// column is secondary, put index key into index tree
	for i, column := range tableDef.Columns {
		if column.IndexType == Secondary {
			indexTree := inedxes[column.Name]
			//LiteralNode := node.Values[i].(LiteralNode)
			//indexKey := LiteralNode.Value.(uint32)
			indexKey := node.Values[i].(uint32)
			indexTree.Insert(indexKey, e.SqlTableManager.serializeInt(key))
		}
	}
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
		if !(col.IndexType == Primary) {
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

func getPrimeryKeyCondition(clause []*BinaryOpNode, definition *SqlTableDefinition, operation TokenType) (*BinaryOpNode, error) {
	priKeyName, err := getPriName(definition)
	if err != nil {
		log.Fatal(err)
	}
	for _, node := range clause {
		if left, ok := node.Left.(*ColumnNode); ok {
			if left.ColumnName == priKeyName && node.Operator == operation {
				return node, nil
			}
		}
	}
	return nil, fmt.Errorf("No primary key column found")
}

func getSecondaryKeyCondition(clause []*BinaryOpNode, definition *SqlTableDefinition, operation TokenType) (*BinaryOpNode, error) {
	secondaryIndexes, err := getSecondaryIndex(definition)
	if err != nil {
		log.Fatal(err)
	}
	for _, node := range clause {
		if left, ok := node.Left.(*ColumnNode); ok {
			if slices.Contains(secondaryIndexes, left.ColumnName) && node.Operator == operation {
				return node, nil
			}
		}
	}
	return nil, fmt.Errorf("No primary key column found")
}

func GetPrimaryTreeRows(tree *disktree.BPTree, priKey uint32, definition *SqlTableDefinition) []map[string]interface{} {
	all, _ := tree.SearchAll(priKey)

	if all != nil {
		rows := make([]map[string]interface{}, 0)
		for _, bytes := range all {
			// deserialize
			rows = append(rows, deserializeRow(definition, bytes))
		}
		logger.Debug("rows: %v \n", rows)
		return rows
	}
	log.Fatal("can't found data")
	return nil
}

func GetSecondaryTreeRows(tree *disktree.BPTree, indexKey uint32, definition *SqlTableDefinition) []map[string]interface{} {
	all, _ := tree.SearchAll(indexKey)
	if all != nil {
		rows := make([]map[string]interface{}, 0)
		for _, bytes := range all {
			// deserialize
			rows = append(rows, deserializeRow(definition, bytes))
		}
		logger.Debug("rows: %v \n", rows)
		return rows
	}
	log.Fatal("can't found data")
	return nil
}

func GetSecondaryTreeRowsFromPri(tree *disktree.BPTree, indexKey uint32, priTree *disktree.BPTree, definition *SqlTableDefinition) []map[string]interface{} {
	allPri, _ := tree.SearchAll(indexKey)
	priKeys := make([]uint32, 0)
	rows := make([]map[string]interface{}, 0)
	if allPri != nil {
		for _, bytes := range allPri {
			priKeys = append(priKeys, DeserializeInt(bytes))
		}
	}
	for _, pri := range priKeys {
		rows = append(rows, GetPrimaryTreeRows(priTree, pri, definition)...)
	}
	return rows
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
		if column.IndexType == Primary {
			return column.Name, nil // 返回列名而不是数据类型
		}
	}
	return "", fmt.Errorf("primary key not exist in table definition")
}

func getSecondaryIndex(definition *SqlTableDefinition) ([]string, error) {
	indexes := []string{}
	for _, column := range definition.Columns {
		if column.IndexType == Secondary {
			indexes = append(indexes, column.Name)
		}
	}
	if len(indexes) == 0 {
		return nil, fmt.Errorf("no primary key column found in table definition")
	}
	return indexes, nil
}
