package database

import (
	. "godb/sqlparser"
)

// @Title        sqlTableDefinition.go
// @Description
// @Create       david 2024-12-31 15:31
// @Update       david 2024-12-31 15:31

type SqlTableDefinition struct {
	TableName string                `json:"tableName"`
	Columns   []SqlColumnDefinition `json:"columns"`
}

func NewSqlTableDefinition(tableName string, columns []*ColumnDefinition) *SqlTableDefinition {
	scd := []SqlColumnDefinition{}
	for _, column := range columns {
		columnDefinition := newSqlColumnDefinition(column.Name, column.DataType, column.PrimaryKey)
		scd = append(scd, *columnDefinition)
	}
	return &SqlTableDefinition{
		TableName: tableName,
		Columns:   scd,
	}
}

func (sd *SqlTableDefinition) String() string {
	return sd.TableName
}
