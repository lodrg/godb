package database

import "godb/sqlparser"

// @Title        executeResult.go
// @Description
// @Create       david 2025-01-02 15:54
// @Update       david 2025-01-02 15:54

type ResultType int

const (
	SELECT ResultType = iota
	INSERT
	CREATE
	ERROR
)

type ExecuteResult struct {
	resultType      ResultType
	rows            *[][]any
	affectedRows    uint32
	tableDefinition *SqlTableDefinition
	slqParsed       *sqlparser.ASTNode
}

func NewExecuteResult(resultType ResultType, rowsData *[][]any, affectedrow uint32, tableDefinition *SqlTableDefinition, sqlParsed *sqlparser.ASTNode) ExecuteResult {
	return ExecuteResult{
		resultType:      resultType,
		rows:            rowsData,
		affectedRows:    affectedrow,
		tableDefinition: tableDefinition,
		slqParsed:       sqlParsed,
	}
}

func ForSelect(rowsData *[][]any, tableDefinition *SqlTableDefinition, sqlParsed *sqlparser.ASTNode) ExecuteResult {
	return NewExecuteResult(SELECT, rowsData, 0, tableDefinition, sqlParsed)
}

func ForInsert(affected uint32, tableDefinition *SqlTableDefinition) ExecuteResult {
	return NewExecuteResult(INSERT, nil, affected, tableDefinition, nil)
}

func ForCreate(tableDefinition *SqlTableDefinition) ExecuteResult {
	return NewExecuteResult(CREATE, nil, 0, tableDefinition, nil)
}
func ForError(errorMessage string) ExecuteResult {
	rows := [][]any{{errorMessage}}
	return NewExecuteResult(ERROR, &rows, 0, nil, nil)
}
