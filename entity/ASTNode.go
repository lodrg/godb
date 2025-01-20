package entity

import (
	"fmt"
	"strconv"
	"strings"
)

// @Title        ASTNode.go
// @Description
// @Create       david 2024-12-27 14:01
// @Update       david 2024-12-27 14:01

type ASTNode interface {
	String() string
}

type BinaryOpNode struct {
	Operator    string
	Left, Right ASTNode
}

func NewBinaryOpNode(operator string, left, right ASTNode) *BinaryOpNode {
	return &BinaryOpNode{
		Operator: operator,
		Left:     left,
		Right:    right,
	}
}

type IdentifierNode struct {
	Name string
}

func newIdentifierNode(name string) *IdentifierNode {
	return &IdentifierNode{
		Name: name,
	}
}

type InsertNode struct {
	TableName string
	Columns   []string
	Values    []interface{}
}

func newInsertNode(tableName string, columns []string, values []interface{}) *InsertNode {
	return &InsertNode{
		TableName: tableName,
		Columns:   columns,
		Values:    values,
	}
}

type SelectNode struct {
	TableName      string
	Columns        []*ColumnNode
	WhereClause    []*BinaryOpNode
	OrderByColumns []*ColumnNode
	Join           []*JoinNode
}

func NewSelectNode(tableName string, columns []*ColumnNode, whereCause []*BinaryOpNode, orderBy []*ColumnNode, join []*JoinNode) *SelectNode {
	return &SelectNode{
		TableName:      tableName,
		Columns:        columns,
		WhereClause:    whereCause,
		OrderByColumns: orderBy,
		Join:           join,
	}
}

type JoinNode struct {
	TableName string
	Condition ASTNode
}

func NewJoinNode(tableName string, condition ASTNode) *JoinNode {
	return &JoinNode{
		TableName: tableName,
		Condition: condition,
	}
}

type LiteralNode struct {
	Value interface{}
}

func NewLiteralNode(value interface{}) *LiteralNode {
	return &LiteralNode{
		Value: value,
	}
}

type ColumnType int

const (
	WILDCARDN ColumnType = iota
	PLAIN_STRING
	TABLE_NAME_PREFIXED
)

type ColumnNode struct {
	TableName  string
	ColumnName string
	ColumnType ColumnType
}

func NewColumnNode(tableName string, columnName string, columnType ColumnType) *ColumnNode {
	return &ColumnNode{
		TableName:  tableName,
		ColumnName: columnName,
		ColumnType: columnType,
	}
}

type CreateTableNode struct {
	TableName string
	Columns   []*ColumnDefinition
}

func NewCreateTableNode(tableName string, columns []*ColumnDefinition) *CreateTableNode {
	return &CreateTableNode{
		TableName: tableName,
		Columns:   columns,
	}
}

// BinaryOpNode
func (n *BinaryOpNode) String() string {
	if n == nil {
		return "<nil>"
	}
	return fmt.Sprintf("(%v %s %v)", n.Left, n.Operator, n.Right)
}

// IdentifierNode
func (n *IdentifierNode) String() string {
	if n == nil {
		return "<nil>"
	}
	return n.Name
}

// InsertNode
func (n *InsertNode) String() string {
	if n == nil {
		return "<nil>"
	}
	var sb strings.Builder
	sb.WriteString("INSERT INTO ")
	sb.WriteString(n.TableName)

	if len(n.Columns) > 0 {
		sb.WriteString(" (")
		sb.WriteString(strings.Join(n.Columns, ", "))
		sb.WriteString(")")
	}

	sb.WriteString(" VALUES (")
	// 转换 interface{} 到字符串
	strValues := make([]string, len(n.Values))
	for i, v := range n.Values {
		switch val := v.(type) {
		case uint32:
			strValues[i] = strconv.FormatUint(uint64(val), 10)
		case string:
			strValues[i] = "'" + val + "'" // 字符串需要加引号
		default:
			strValues[i] = fmt.Sprintf("%v", val)
		}
	}

	sb.WriteString(strings.Join(strValues, ", "))
	sb.WriteString(")")

	return sb.String()
}

// SelectNode
func (n *SelectNode) String() string {
	if n == nil {
		return "<nil>"
	}
	var sb strings.Builder
	sb.WriteString("SELECT ")

	// Columns
	if len(n.Columns) == 0 {
		sb.WriteString("*")
	} else {
		cols := make([]string, len(n.Columns))
		for i, col := range n.Columns {
			cols[i] = col.String()
		}
		sb.WriteString(strings.Join(cols, ", "))
	}

	sb.WriteString(" FROM ")
	sb.WriteString(n.TableName)

	// Joins
	for _, join := range n.Join {
		sb.WriteString(" ")
		sb.WriteString(join.String())
	}

	// Where clause
	if len(n.WhereClause) > 0 {
		sb.WriteString(" WHERE ")
		conditions := make([]string, len(n.WhereClause))
		for i, cond := range n.WhereClause {
			conditions[i] = cond.String()
		}
		sb.WriteString(strings.Join(conditions, " AND "))
	}

	// Order by
	if len(n.OrderByColumns) > 0 {
		sb.WriteString(" ORDER BY ")
		cols := make([]string, len(n.OrderByColumns))
		for i, col := range n.OrderByColumns {
			cols[i] = col.String()
		}
		sb.WriteString(strings.Join(cols, ", "))
	}

	return sb.String()
}

// JoinNode
func (n *JoinNode) String() string {
	if n == nil {
		return "<nil>"
	}
	return fmt.Sprintf("JOIN %s ON %v", n.TableName, n.Condition)
}

// LiteralNode
func (n *LiteralNode) String() string {
	if n == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%v", n.Value)
}

// ColumnNode
func (n *ColumnNode) String() string {
	if n == nil {
		return "<nil>"
	}
	switch n.ColumnType {
	case WILDCARDN:
		return "*"
	case PLAIN_STRING:
		return n.ColumnName
	case TABLE_NAME_PREFIXED:
		return fmt.Sprintf("%s.%s", n.TableName, n.ColumnName)
	default:
		return fmt.Sprintf("UNKNOWN_COLUMN_TYPE(%s.%s)", n.TableName, n.ColumnName)
	}
}

// CreateTableNode
func (n *CreateTableNode) String() string {
	if n == nil {
		return "<nil>"
	}
	var sb strings.Builder
	sb.WriteString("CREATE TABLE ")
	sb.WriteString(n.TableName)
	sb.WriteString(" (\n")

	cols := make([]string, len(n.Columns))
	for i, col := range n.Columns {
		cols[i] = "  " + col.String()
	}

	sb.WriteString(strings.Join(cols, ",\n"))
	sb.WriteString("\n)")

	return sb.String()
}
