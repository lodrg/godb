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
	Res_SELECT ResultType = iota
	Res_INSERT
	Res_CREATE
	Res_ERROR
)

type ExecuteResult struct {
	resultType       ResultType
	rows             map[string]interface{}
	affectedRows     uint32
	tableDefinitions []*SqlTableDefinition
	slqParsed        *sqlparser.ASTNode
}

func NewExecuteResult(resultType ResultType, rowsData map[string]interface{}, affectedrow uint32, tableDefinitions []*SqlTableDefinition, sqlParsed *sqlparser.ASTNode) ExecuteResult {
	return ExecuteResult{
		resultType:       resultType,
		rows:             rowsData,
		affectedRows:     affectedrow,
		tableDefinitions: tableDefinitions,
		slqParsed:        sqlParsed,
	}
}

func ForSelect(rowsData map[string]interface{}, tableDefinitions []*SqlTableDefinition, sqlParsed *sqlparser.ASTNode) ExecuteResult {
	return NewExecuteResult(Res_SELECT, rowsData, 0, tableDefinitions, sqlParsed)
}

func ForInsert(affected uint32, tableDefinitions []*SqlTableDefinition) ExecuteResult {
	return NewExecuteResult(Res_INSERT, nil, affected, tableDefinitions, nil)
}

func ForCreate(tableDefinitions []*SqlTableDefinition) ExecuteResult {
	return NewExecuteResult(Res_CREATE, nil, 0, tableDefinitions, nil)
}
func ForError(errorMessage string) ExecuteResult {
	rows := map[string]interface{}{"error": errorMessage}
	return NewExecuteResult(Res_ERROR, rows, 0, nil, nil)
}

func (r ExecuteResult) String() string {
	switch r.resultType {
	case Res_SELECT:
		return r.formatSelectResult()
	case Res_INSERT:
		return r.formatInsertResult()
	case Res_CREATE:
		return r.formatCreateResult()
	case Res_ERROR:
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
	if len(r.tableDefinitions) == 0 {
		return "Table created"
	}

	var sb strings.Builder
	tableDef := r.tableDefinitions[0]

	// 添加基本信息
	sb.WriteString(fmt.Sprintf("CREATE TABLE Result: Table '%s' created successfully.\n", tableDef.TableName))
	sb.WriteString("Columns:\n")

	// 添加列信息
	for _, col := range tableDef.Columns {
		sb.WriteString(fmt.Sprintf("  - %s (%s)", col.Name, col.DataType))
		if col.IsPrimaryKey {
			sb.WriteString(" PRIMARY KEY")
		}
		sb.WriteString("\n")
	}

	return sb.String()
}

// 格式化 ERROR 结果
func (r ExecuteResult) formatErrorResult() string {
	if errorMsg, ok := r.rows["error"].(string); ok {
		return fmt.Sprintf("ERROR: %s", errorMsg)
	}
	return "ERROR: Unknown error occurred"
}
