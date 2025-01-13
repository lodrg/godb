package database

import (
	"fmt"
	"godb/sqlparser"
	"strings"
)

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
	rows            map[string]interface{}
	affectedRows    uint32
	tableDefinition *SqlTableDefinition
	slqParsed       *sqlparser.ASTNode
}

func NewExecuteResult(resultType ResultType, rowsData map[string]interface{}, affectedrow uint32, tableDefinition *SqlTableDefinition, sqlParsed *sqlparser.ASTNode) ExecuteResult {
	return ExecuteResult{
		resultType:      resultType,
		rows:            rowsData,
		affectedRows:    affectedrow,
		tableDefinition: tableDefinition,
		slqParsed:       sqlParsed,
	}
}

func ForSelect(rowsData map[string]interface{}, tableDefinition *SqlTableDefinition, sqlParsed *sqlparser.ASTNode) ExecuteResult {
	return NewExecuteResult(SELECT, rowsData, 0, tableDefinition, sqlParsed)
}

func ForInsert(affected uint32, tableDefinition *SqlTableDefinition) ExecuteResult {
	return NewExecuteResult(INSERT, nil, affected, tableDefinition, nil)
}

func ForCreate(tableDefinition *SqlTableDefinition) ExecuteResult {
	return NewExecuteResult(CREATE, nil, 0, tableDefinition, nil)
}
func ForError(errorMessage string) ExecuteResult {
	rows := map[string]interface{}{"error": errorMessage}
	return NewExecuteResult(ERROR, rows, 0, nil, nil)
}

func (r ExecuteResult) String() string {
	switch r.resultType {
	case SELECT:
		return r.formatSelectResult()
	case INSERT:
		return r.formatInsertResult()
	case CREATE:
		return r.formatCreateResult()
	case ERROR:
		return r.formatErrorResult()
	default:
		return "Unknown result type"
	}
}

// 格式化 SELECT 结果
func (r ExecuteResult) formatSelectResult() string {
	if r.rows == nil || len(r.rows) == 0 {
		return "Empty set"
	}

	var result strings.Builder
	result.WriteString(fmt.Sprintf("%d row(s) in set\n", len(r.rows)))

	// 如果需要显示具体数据，可以遍历 r.rows
	for key, value := range r.rows {
		result.WriteString(fmt.Sprintf("%v: %v\n", key, value))
	}

	return result.String()
}

// 格式化 INSERT 结果
func (r ExecuteResult) formatInsertResult() string {
	return fmt.Sprintf("Query OK, %d row(s) affected", r.affectedRows)
}

// 格式化 CREATE 结果
func (r ExecuteResult) formatCreateResult() string {
	if r.tableDefinition == nil {
		return "Table created"
	}
	return fmt.Sprintf("Table '%s' created", r.tableDefinition.TableName)
}

// 格式化 ERROR 结果
func (r ExecuteResult) formatErrorResult() string {
	if errorMsg, ok := r.rows["error"].(string); ok {
		return fmt.Sprintf("ERROR: %s", errorMsg)
	}
	return "ERROR: Unknown error occurred"
}
