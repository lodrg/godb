package database

// @Title        sqlTableDefinition.go
// @Description
// @Create       david 2024-12-31 15:31
// @Update       david 2024-12-31 15:31

type SqlTableDefinition struct {
	tableName string
	columns   []SqlColumnDefinition
}

func NewSqlTableDefinition(tableName string) *SqlTableDefinition {
	return &SqlTableDefinition{
		tableName: tableName,
		columns:   make([]SqlColumnDefinition, 0),
	}
}
