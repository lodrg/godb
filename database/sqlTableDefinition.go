package database

// @Title        sqlTableDefinition.go
// @Description
// @Create       david 2024-12-31 15:31
// @Update       david 2024-12-31 15:31

type SqlTableDefinition struct {
	TableName string                `json:"tableName"`
	Columns   []SqlColumnDefinition `json:"columns"`
}

func NewSqlTableDefinition(tableName string) *SqlTableDefinition {
	return &SqlTableDefinition{
		TableName: tableName,
		Columns:   make([]SqlColumnDefinition, 0),
	}
}
