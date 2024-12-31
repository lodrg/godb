package lpjsonparser

// @Title        ColumnDefinition.go
// @Description
// @Create       david 2024-12-27 18:03
// @Update       david 2024-12-27 18:03

type ColumnDefinition struct {
	Name       string
	DataType   TokenType
	PrimaryKey bool
}
