package lpjsonparser

// @Title        ASTNode.go
// @Description
// @Create       david 2024-12-27 14:01
// @Update       david 2024-12-27 14:01

type ASTNode interface{}

type BinaryOpNode struct {
	Operator    string
	Left, Right ASTNode
}

func newBinaryOpNode(operator string, left, right ASTNode) *BinaryOpNode {
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
	Values    []string
}

func newInsertNode(tableName string, columns []string, values []string) *InsertNode {
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

func newSelectNode(tableName string, columns []*ColumnNode, whereCause []*BinaryOpNode, orderBy []*ColumnNode, join []*JoinNode) *SelectNode {
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

func newJoinNode(tableName string, condition ASTNode) *JoinNode {
	return &JoinNode{
		TableName: tableName,
		Condition: condition,
	}
}

type LiteralNode struct {
	Value interface{}
}

func newLiteralNode(value interface{}) *LiteralNode {
	return &LiteralNode{}
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

func newColumnNode(tableName string, columnName string, columnType ColumnType) *ColumnNode {
	return &ColumnNode{
		TableName:  tableName,
		ColumnName: columnName,
		ColumnType: columnType,
	}
}

type CreateTbaleNode struct {
	TableName string
	Columns   []*ColumnDefinition
}

func newCreateTbaleNode(tableName string, columns []*ColumnDefinition) *CreateTbaleNode {
	return &CreateTbaleNode{
		TableName: tableName,
		Columns:   columns,
	}
}
