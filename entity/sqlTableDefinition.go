package entity

// @Title        sqlTableDefinition.go
// @Description
// @Create       david 2024-12-31 15:31
// @Update       david 2024-12-31 15:31

type SqlTableDefinition struct {
	TableName string              `json:"tableName"`
	Columns   []*ColumnDefinition `json:"columns"`
}

func NewSqlTableDefinition(tableName string, columns []*ColumnDefinition) *SqlTableDefinition {
	return &SqlTableDefinition{
		TableName: tableName,
		Columns:   columns,
	}
}

func (sd *SqlTableDefinition) String() string {
	return sd.TableName
}
