package database

// @Title        slqColumnDefinition.go
// @Description
// @Create       david 2024-12-31 15:33
// @Update       david 2024-12-31 15:33

import (
	"godb/sqlparser"
)

type SqlColumnDefinition struct {
	name         string
	dataType     sqlparser.TokenType
	isPrimaryKey bool
}

func NewSqlColumnDefinition(name string, dataType sqlparser.TokenType, isPrimaryKey bool) *SqlColumnDefinition {
	return &SqlColumnDefinition{
		name:         name,
		dataType:     dataType,
		isPrimaryKey: isPrimaryKey,
	}
}
