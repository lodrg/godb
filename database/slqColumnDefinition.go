package database

// @Title        slqColumnDefinition.go
// @Description
// @Create       david 2024-12-31 15:33
// @Update       david 2024-12-31 15:33

import (
	"godb/sqlparser"
)

type SqlColumnDefinition struct {
	Name         string              `json:"name"`
	DataType     sqlparser.TokenType `json:"dataType"`
	IsPrimaryKey bool                `json:"primaryKey"`
}

func NewSqlColumnDefinition(name string, dataType sqlparser.TokenType, isPrimaryKey bool) *SqlColumnDefinition {
	return &SqlColumnDefinition{
		Name:         name,
		DataType:     dataType,
		IsPrimaryKey: isPrimaryKey,
	}
}
