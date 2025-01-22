package database

import (
	"fmt"
	. "godb/entity"
	. "godb/sqlparser"
	"log"
)

// @Title        database.go
// @Description
// @Create       david 2024-12-31 15:25
// @Update       david 2024-12-31 15:25

type DataBase struct {
	sqlTableManager  *SqlTableManager
	sqlTableExecutor *SqlQueryExecutor
}

func NewDataBase(dataDirectory string) *DataBase {
	manager := NewSqlTableManager(dataDirectory)
	executor := NewSqlQueryExecutor(manager)
	return &DataBase{
		sqlTableManager:  manager,
		sqlTableExecutor: executor,
	}
}

func (b *DataBase) Execute(sql string) (ExecuteResult, error) {
	ASTNode, err := Parse(sql)
	if err != nil {
		log.Fatal(err)
	}
	switch Node := ASTNode.(type) {
	case *SelectNode:
		sqlTableDefinitions := make([]*SqlTableDefinition, 0)
		result := b.sqlTableExecutor.processSelect(Node, sqlTableDefinitions)
		return ForSelect(result, sqlTableDefinitions, &ASTNode), nil
	case *InsertNode:
		sqlTableDefinitions := make([]*SqlTableDefinition, 0)
		affectedrows := b.sqlTableExecutor.processInsert(Node, sqlTableDefinitions)
		return ForInsert(affectedrows, sqlTableDefinitions), nil
	case *CreateTableNode:
		sqlTableDefinitions := make([]*SqlTableDefinition, 0)
		b.sqlTableExecutor.prcessCreateTable(Node, sqlTableDefinitions)
		return ForCreate(sqlTableDefinitions), nil
	default:
		err := fmt.Errorf("Unknown node type: %T", ASTNode)
		return ForError(err.Error()), err
	}
}

func (b *DataBase) Close() {
	b.sqlTableManager.Close()
}
