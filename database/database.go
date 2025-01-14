package database

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"godb/disktree"
	f "godb/file"
	"godb/logger"
	"godb/sqlparser"
	"log"
	"os"
	"path/filepath"
	"strconv"
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

func (b *DataBase) Execute(sql string) (ExecuteResult, error) {
	ASTNode, err := sqlparser.Parse(sql)
	if err != nil {
		log.Fatal(err)
	}
	switch Node := ASTNode.(type) {
	case *sqlparser.SelectNode:
		result := b.processSelect(Node)
		return ForSelect(result, b.tableDefinitions[Node.TableName], &ASTNode), nil
	case *sqlparser.InsertNode:
		affectedrows := b.processInsert(Node)
		return ForInsert(affectedrows, b.tableDefinitions[Node.TableName]), nil
	case *sqlparser.CreateTableNode:
		b.prcessCreateTable(Node)
		return ForCreate(b.tableDefinitions[Node.TableName]), nil
	default:
		err := fmt.Errorf("Unknown node type: %T", ASTNode)
		return ForError(err.Error()), err
	}
}

func (b *DataBase) processSelect(node *sqlparser.SelectNode) map[string]interface{} {
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

func (b *DataBase) processInsert(node *sqlparser.InsertNode) uint32 {
	// get table def form json
	tableDefinition := b.tableDefinitions[node.TableName]
	// get tree
	tree := b.tableTrees[node.TableName]

	// format values
	values := make(map[string]interface{}, 0)

	if len(node.Columns) == 0 {
		// no columns in sql
		if len(node.Values) != len(tableDefinition.Columns) {
			panic(fmt.Sprintf("value count (%d) doesn't match column count (%d)",
				len(node.Values), len(tableDefinition.Columns)))
		}
		for i, column := range tableDefinition.Columns {
			values[column.Name] = node.Values[i]
		}
	} else {
		// have columns in sql
		if len(node.Values) != len(node.Columns) {
			panic(fmt.Sprintf("value count (%d) doesn't match column count (%d)",
				len(node.Values), len(tableDefinition.Columns)))
		}
		for i, colName := range node.Columns {
			values[colName] = node.Values[i]
		}

		// every values is needed in sql
		// TODO: support null in future
		for _, col := range tableDefinition.Columns {
			if _, exists := values[col.Name]; !exists {
				panic(fmt.Sprintf("missing value for column %s", col.Name))
			}
		}
	}

	// find pk value
	var pkValue string
	var pkColumn SqlColumnDefinition
	var foundPK bool

	for _, col := range tableDefinition.Columns {
		if col.IsPrimaryKey {
			if val, exists := values[col.Name]; exists {
				switch col.DataType {
				case TypeInt:
					if intVal, ok := val.(uint32); ok {
						pkValue = strconv.FormatUint(uint64(intVal), 10)
					} else {
						panic(fmt.Sprintf("invalid type for primary key column %s, expected uint32", col.Name))
					}
				case TypeChar:
					if strVal, ok := val.(string); ok {
						pkValue = strVal
					} else {
						panic(fmt.Sprintf("invalid type for primary key column %s, expected string", col.Name))
					}
				default:
					panic(fmt.Sprintf("unsupported primary key type for column %s", col.Name))
				}
				pkColumn = col
				foundPK = true
				break
			}
			panic(fmt.Sprintf("missing value for column %s", col.Name))
		}
	}

	if !foundPK {
		panic(fmt.Sprintf("missing value for column %s", pkColumn.Name))
	}

	// only uint32 as pk
	val, err := strconv.ParseUint(pkValue, 10, 32)
	if err != nil {
		panic(fmt.Sprintf("invalid primary key value: %s (must be a positive integer <= %d)",
			pkValue, uint32(^uint32(0))))
	}
	key := uint32(val)

	if _, exists := tree.Search(key); exists {
		log.Fatal("duplicate primary key found")
	}

	bufRecord := b.serializeRow(values, tableDefinition)
	if err != nil {
		log.Fatal(err)
	}

	rows := tree.Insert(key, bufRecord.Bytes())
	//panic("TODO: processInsert")
	return rows
}

func (b *DataBase) prcessCreateTable(node *sqlparser.CreateTableNode) {
	panic("TODO: processCreate")
}

func (b *DataBase) getPrimeryKeyCondition(clause []*sqlparser.BinaryOpNode, definition *SqlTableDefinition) (*sqlparser.BinaryOpNode, error) {
	priKeyName, err := getPriName(definition)
	if err != nil {
		log.Fatal(err)
	}
	for _, node := range clause {
		if left, ok := node.Left.(*sqlparser.ColumnNode); ok {
			if left.ColumnName == priKeyName {
				return node, nil
			}
		}
	}
	return nil, fmt.Errorf("No primary key column found")
}

func (b *DataBase) getRows(tree *disktree.BPTree, condition *sqlparser.BinaryOpNode, definition *SqlTableDefinition) []map[string]interface{} {
	right := condition.Right.(*sqlparser.LiteralNode)
	logger.Debug("rightvalue: %v \n", right.Value)
	priKey := right.Value.(uint32)
	logger.Debug("priKey: %v \n", priKey)
	all, _ := tree.SearchAll(priKey)
	logger.Debug("all: %v \n", all)
	logger.Debug("all: %x \n", all)
	logger.Debug("df")

	if all != nil {
		rows := make([]map[string]interface{}, 0)
		for _, bytes := range all.([][]byte) {
			// deserialize
			rows = append(rows, deserializeRow(definition, bytes))
		}
		//fmt.Printf("rows: %v \n", rows)
		logger.Debug("rows: %v \n", rows)
		return rows
	}
	log.Fatal("can't found data")
	return nil
}

func (b *DataBase) makeBufferRecord(definition *SqlTableDefinition, values map[string]string) (bytes.Buffer, error) {
	record := make([]string, len(definition.Columns))
	for i, col := range definition.Columns {
		if val, exists := values[col.Name]; exists {
			// 验证值是否符合列的数据类型
			if err := col.ValidateValue(val); err != nil {
				panic(err.Error())
			}
			record[i] = val
		} else {
			//if !col.IsNullable {
			//	panic(fmt.Sprintf("missing value for non-nullable column %s", col.Name))
			//}
			record[i] = ""
		}
	}

	// 将记录序列化为[]byte
	var buf bytes.Buffer

	// 写入字段数量
	binary.Write(&buf, binary.LittleEndian, uint16(len(record)))

	// 写入每个字段
	for _, field := range record {
		// 写入字段长度
		binary.Write(&buf, binary.LittleEndian, uint16(len(field)))
		// 写入字段内容
		buf.WriteString(field)
	}

	return buf, nil
}

func (b *DataBase) serializeRow(record map[string]interface{}, definition *SqlTableDefinition) *bytes.Buffer {
	buf := new(bytes.Buffer)

	for _, column := range definition.Columns {
		switch column.DataType {
		case TypeInt:
			// 写入整数，固定4字节
			value := record[column.Name].(uint32) // 类型断言
			data := make([]byte, INT_SIZE)
			binary.BigEndian.PutUint32(data, value)
			buf.Write(data)

		case TypeChar:
			// 写入字符串，固定长度(CHAR_SIZE + CHAR_LENGTH)
			value := record[column.Name].(string)
			// 创建固定长度的字节数组
			data := make([]byte, CHAR_SIZE+CHAR_LENGTH)
			// 复制字符串内容，如果超出长度会被截断
			copy(data, []byte(value))
			buf.Write(data)

		default:
			log.Fatal("SerializeRow Unknown column type:", column.DataType)
		}
	}

	return buf
}

func deserializeRow(definition *SqlTableDefinition, bytes []byte) map[string]interface{} {
	// check row size
	rowSize := getRowSize(definition)
	if rowSize < len(bytes) {
		log.Fatalf("Row size mismatch, row size: %d, expected row size: %d", len(bytes), rowSize)
	}

	// from bytes to typed data
	result := make(map[string]interface{})
	columns := definition.Columns
	curPosition := 0

	for _, column := range columns {
		switch column.DataType {
		case TypeInt:
			// 将4个字节转换为uint32
			if curPosition+INT_SIZE <= len(bytes) {
				value := binary.BigEndian.Uint32(bytes[curPosition : curPosition+INT_SIZE])
				result[column.Name] = value
				curPosition += INT_SIZE
			}

		case TypeChar:
			// 处理字符串类型，去除空字节
			if curPosition+CHAR_SIZE+CHAR_LENGTH <= len(bytes) {
				strBytes := bytes[curPosition : curPosition+CHAR_SIZE+CHAR_LENGTH]
				// 找到第一个null字节或者结束位置
				endPos := 0
				for i, b := range strBytes {
					if b == 0 {
						endPos = i
						break
					}
				}
				if endPos == 0 {
					endPos = len(strBytes)
				}
				result[column.Name] = string(strBytes[:endPos])
				curPosition += CHAR_LENGTH + CHAR_LENGTH
			}

		default:
			log.Fatal("DeserializeRow Unknown column type:", column.DataType)
		}
	}
	return result
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
