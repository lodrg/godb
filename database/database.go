package database

import (
	"fmt"
	. "godb/entity"
	"godb/logger"
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
	logger.Debug("start execute sql: %v \n", sql)
	ASTNode, err := Parse(sql)
	if err != nil {
		log.Fatal(err)
	}
	logger.Debug("finish parse sql to ASTNode")
	switch Node := ASTNode.(type) {
	case *SelectNode:
		logger.Info("start execute select sql: %s \n", sql)
		sqlTableDefinitions := make([]*SqlTableDefinition, 0)
		result := b.sqlTableExecutor.processSelect(Node, sqlTableDefinitions)
		return ForSelect(result, sqlTableDefinitions, &ASTNode), nil
	case *InsertNode:
		logger.Info("start execute insert sql: %s \n", sql)
		sqlTableDefinitions := make([]*SqlTableDefinition, 0)
		affectedrows := b.sqlTableExecutor.processInsert(Node, sqlTableDefinitions)
		return ForInsert(affectedrows, sqlTableDefinitions), nil
	case *UpdateNode:
		logger.Info("start execute insert sql: %s \n", sql)
		sqlTableDefinitions := make([]*SqlTableDefinition, 0)
		result := b.sqlTableExecutor.processUpdate(Node, sqlTableDefinitions)
		return ForUpdate(result, sqlTableDefinitions), nil
	case *CreateTableNode:
		logger.Info("start execute create sql: %s \n", sql)
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
